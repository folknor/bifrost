//! Supported in-process test seam for crates that ride `bifrost-net`.
//!
//! Account crates cannot construct a [`crate::Response`] directly - it
//! is `#[non_exhaustive]` by design, so the buffered response shape can
//! grow without breaking consumers. That left every net-riding crate
//! building its own private double, and each one had to re-derive
//! bifrost-net's status contract from scratch. The contract is easy to
//! get wrong in a way that hides live defects: the retry loop returns
//! `Err` for every 4xx/5xx, so a hand-rolled double that hands back a
//! 404 as `Ok(Response)` pins a branch production can never reach.
//!
//! This module removes the need to re-derive it. A canned response is
//! staged at the *wire* boundary, below retry, redirect, rate limiting,
//! and bandwidth metering, so a scripted test drives the real pipeline
//! and observes whatever the real pipeline would have produced.
//!
//! The dispatch trait itself stays crate-private: its signature is in
//! terms of `reqwest` types, and keeping `reqwest` out of the public
//! API is the reason this crate's request wrapper exists at all. What
//! is public is the scripted double and the `Net` constructor that
//! installs it.
//!
//! Gated behind the `test-support` feature. Consumers enable it as a
//! dev-dependency:
//!
//! ```toml
//! [dev-dependencies]
//! bifrost-net = { path = "../net", features = ["test-support"] }
//! ```

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use bifrost_types::{AccountFuture, AccountId};
use bytes::Bytes;
use reqwest::StatusCode;
use reqwest::header::HeaderMap;

use crate::auth::TokenSource;
use crate::config::NetConfig;
use crate::error::Error;
use crate::net::{AccountNet, AccountSpec, Net};
use crate::rate::RateLimit;
use crate::request::{Dispatch, send_error_to_error};
use crate::retry::RetryPolicy;

/// One scripted wire outcome, consumed by the next dispatch.
///
/// Note what a `Response` means here: it is what the socket produced,
/// *not* what `send()` returns. bifrost-net's retry loop converts every
/// 4xx and 5xx into `Err`, so scripting a `Response` with a 404 exercises
/// the error path, not a success path. A 2xx and a passed-through 3xx are
/// the only statuses that surface to a caller as `Ok`.
pub enum Canned {
    /// A response the server produced, with the given status, headers,
    /// and body.
    Response {
        /// Status line.
        status: StatusCode,
        /// Response headers.
        headers: HeaderMap,
        /// Body bytes.
        body: Bytes,
    },
    /// A transport-level failure, before any response exists. Drives
    /// the network-retry lane rather than the status lane.
    Error(Error),
    /// A dispatch that never completes. Drives timeout and cancellation
    /// paths.
    Pending,
}

/// What a scripted dispatch actually received on the wire, captured
/// before the canned outcome is produced.
#[derive(Clone)]
pub struct RequestSnapshot {
    /// HTTP method as sent.
    pub method: reqwest::Method,
    /// Fully resolved request URL, after any redirect hop.
    pub url: reqwest::Url,
    /// Headers as sent, including the transport-injected
    /// `Authorization` and `traceparent`.
    pub headers: HeaderMap,
    /// Request body, if the request carried one in memory.
    pub body: Option<Bytes>,
}

/// A wire dispatcher that answers from a fixed script and records
/// every request it saw.
///
/// An exhausted script panics rather than falling through to the
/// network. A test that under-scripts is a test whose remaining
/// assertions were about to be meaningless, and a silent fallthrough
/// would let it reach a real socket.
pub struct ScriptedDispatch {
    steps: Mutex<VecDeque<Canned>>,
    requests: Mutex<Vec<RequestSnapshot>>,
}

impl ScriptedDispatch {
    /// Build a dispatcher that answers the given outcomes in order.
    #[must_use]
    pub fn new(steps: impl IntoIterator<Item = Canned>) -> Arc<Self> {
        Arc::new(Self {
            steps: Mutex::new(steps.into_iter().collect()),
            requests: Mutex::new(Vec::new()),
        })
    }

    /// Append further outcomes to the end of the script.
    ///
    /// For consumers whose scripting API is called more than once per
    /// test (one call per wire surface, say) and which need all of them
    /// answered by the single dispatcher their client is attached to.
    /// Appending keeps one total order, which is what the wire has.
    pub fn extend(&self, steps: impl IntoIterator<Item = Canned>) {
        self.steps
            .lock()
            .expect("scripted step lock poisoned")
            .extend(steps);
    }

    /// Every request the dispatcher has seen, in order.
    #[must_use]
    pub fn requests(&self) -> Vec<RequestSnapshot> {
        self.requests
            .lock()
            .expect("scripted request lock poisoned")
            .clone()
    }

    /// How many scripted outcomes remain unconsumed. A test that
    /// wants to prove the pipeline stopped early asserts on this.
    #[must_use]
    pub fn remaining(&self) -> usize {
        self.steps
            .lock()
            .expect("scripted step lock poisoned")
            .len()
    }
}

impl Dispatch for ScriptedDispatch {
    fn send(
        &self,
        request: reqwest::RequestBuilder,
    ) -> AccountFuture<Result<reqwest::Response, Error>> {
        let request = match request.build() {
            Ok(request) => request,
            Err(error) => return Box::pin(async move { Err(send_error_to_error(error)) }),
        };
        let body = request
            .body()
            .and_then(reqwest::Body::as_bytes)
            .map(Bytes::copy_from_slice);
        self.requests
            .lock()
            .expect("scripted request lock poisoned")
            .push(RequestSnapshot {
                method: request.method().clone(),
                url: request.url().clone(),
                headers: request.headers().clone(),
                body,
            });
        let step = self
            .steps
            .lock()
            .expect("scripted step lock poisoned")
            .pop_front()
            .expect("scripted dispatch exhausted");
        Box::pin(async move {
            match step {
                Canned::Response {
                    status,
                    headers,
                    body,
                } => {
                    let mut response = http::Response::builder().status(status);
                    for (name, value) in &headers {
                        response = response.header(name, value);
                    }
                    Ok(response.body(body).expect("valid canned response").into())
                }
                Canned::Error(error) => Err(error),
                Canned::Pending => futures::future::pending().await,
            }
        })
    }
}

/// A canned response with the given status and body and no headers.
#[must_use]
pub fn canned(status: StatusCode, body: &'static [u8]) -> Canned {
    Canned::Response {
        status,
        headers: HeaderMap::new(),
        body: Bytes::from_static(body),
    }
}

/// A canned response carrying headers - `Retry-After`, `Location`,
/// `WWW-Authenticate`, and friends.
#[must_use]
pub fn canned_with_headers(status: StatusCode, headers: HeaderMap, body: &'static [u8]) -> Canned {
    Canned::Response {
        status,
        headers,
        body: Bytes::from_static(body),
    }
}

/// Build a `Net` whose wire dispatch answers from `script`.
///
/// Everything above the wire - retry budget, `Retry-After` honor,
/// per-host rate limiting, the redirect walk, bandwidth metering,
/// token refresh - is the production pipeline.
///
/// # Panics
/// Panics if the supplied `NetConfig` cannot build a reqwest client.
/// The client is still constructed (the scripted dispatcher replaces
/// only the send call), so a config that fails here would fail in
/// production too.
#[must_use]
pub fn scripted_net(script: &Arc<ScriptedDispatch>, config: NetConfig) -> Net {
    Net::new_with_dispatch(config, Arc::clone(script) as Arc<dyn Dispatch>)
        .expect("scripted net builds")
}

/// Attach one account to a scripted `Net` and return its `AccountNet`.
///
/// The convenience path for the common case: one account, one script.
/// Tests needing several accounts on a shared governor should call
/// [`scripted_net`] and attach each themselves.
///
/// # Panics
/// Panics if the supplied `NetConfig` cannot build a reqwest client.
#[must_use]
pub fn scripted_account(
    script: &Arc<ScriptedDispatch>,
    config: NetConfig,
    hosts: Vec<RateLimit>,
    token_source: Arc<dyn TokenSource>,
    default_retry: RetryPolicy,
) -> AccountNet {
    scripted_net(script, config).attach_account(
        AccountId("scripted".to_string()),
        AccountSpec {
            hosts,
            token_source,
            default_retry,
        },
    )
}
