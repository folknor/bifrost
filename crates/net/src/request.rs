//! Per-request builder, finished response shapes, and the streaming
//! body adapter.
//!
//! Protocol crates never see a `reqwest::RequestBuilder` directly.
//! Everything routes through this wrapper so the underlying HTTP
//! stack can be swapped without touching call sites.

use std::error::Error as StdError;
use std::pin::Pin;
use std::time::Duration;

use bifrost_types::{AccountFuture, TransmissionState};
use bytes::Bytes;
use futures::Stream;
use reqwest::{
    StatusCode,
    header::{AUTHORIZATION, HeaderMap, HeaderName, HeaderValue, RETRY_AFTER},
};
use serde::Serialize;

use crate::auth::AccessToken;
use crate::error::{Error, FinalResponse, STATUS_BODY_CAP};
use crate::net::{AccountNet, into_byte_stream, wrap_metered};
use crate::rate::RateLimitGovernor;
use crate::redirect::{FollowRedirects, RedirectAction, RedirectPolicy, classify_redirect};
use crate::retry::RetryPolicy;

/// Erased byte-chunk stream, as returned by `AccountNet::download_stream`.
/// One element per chunk reqwest yields off the underlying socket;
/// bandwidth metering wraps every chunk.
pub type ByteStream = Pin<Box<dyn Stream<Item = Result<Bytes, Error>> + Send + 'static>>;

pub(crate) trait Dispatch: Send + Sync + 'static {
    fn send(
        &self,
        request: reqwest::RequestBuilder,
    ) -> AccountFuture<Result<reqwest::Response, Error>>;
}

pub(crate) struct ReqwestDispatch;

impl Dispatch for ReqwestDispatch {
    fn send(
        &self,
        request: reqwest::RequestBuilder,
    ) -> AccountFuture<Result<reqwest::Response, Error>> {
        Box::pin(async move { request.send().await.map_err(send_error_to_error) })
    }
}

struct RateDebit {
    governor: RateLimitGovernor,
    host: String,
    cost: u32,
    armed: bool,
}

impl RateDebit {
    fn new(governor: RateLimitGovernor, host: String, cost: u32) -> Self {
        Self {
            governor,
            host,
            cost,
            armed: true,
        }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for RateDebit {
    fn drop(&mut self) {
        if self.armed {
            self.governor.refund(&self.host, self.cost);
        }
    }
}

/// Fluent request builder. Consumes `self` on every setter so the
/// final `send` call is a single move.
pub struct RequestBuilder {
    /// Method + URL + headers + body, captured opaquely. Replaced
    /// piecewise as the caller chains setters.
    inner: RequestBuilderInner,
}

/// Backing state for the builder. Kept private so the request
/// pipeline can replace the representation without touching call
/// sites.
struct RequestBuilderInner {
    /// Account-scoped transport handle. Carries the reqwest client,
    /// token source, default retry, governor, and bandwidth meter.
    account: AccountNet,
    /// HTTP method.
    method: reqwest::Method,
    /// Target URL.
    url: String,
    /// Caller-provided headers. Merged with transport-injected
    /// `Authorization` and `traceparent` at send time.
    headers: HeaderMap,
    /// Body bytes, if any.
    body: Option<Bytes>,
    /// Optional per-request cost override; otherwise the host's
    /// default cost is used.
    cost: Option<u32>,
    /// Optional per-request retry policy override.
    retry: Option<RetryPolicy>,
    /// Optional per-request timeout override.
    timeout: Option<Duration>,
    /// Caller's override of whether replaying this request can change
    /// server state. `None` derives it from the HTTP method.
    idempotent: Option<bool>,
    /// Whether the transport should inject `Authorization: Bearer`.
    /// Some protocol flows use a pre-authenticated upload URL or an
    /// explicit Basic authorization header, but still need the shared
    /// retry, rate-limit, and metering pipeline.
    bearer_auth: bool,
    /// Deferred error captured at a fluent-setter call site (e.g.
    /// `json()` could not serialize the body). The fluent API does
    /// not return `Result` on every setter, so the error is stashed
    /// here and surfaced from the subsequent `send` / `send_streaming`
    /// before any network call. Only the first error is retained; if
    /// a later setter fails too, we keep the earliest one because
    /// that is what callers tend to debug first.
    pending_error: Option<Error>,
}

impl RequestBuilder {
    /// Construct a builder from an account-scoped transport, method,
    /// and URL. The account-scoped wrappers (`AccountNet::get` etc.)
    /// call this.
    pub(crate) fn new(account: AccountNet, method: reqwest::Method, url: &str) -> Self {
        Self {
            inner: RequestBuilderInner {
                account,
                method,
                url: url.to_owned(),
                headers: HeaderMap::new(),
                body: None,
                cost: None,
                retry: None,
                timeout: None,
                idempotent: None,
                bearer_auth: true,
                pending_error: None,
            },
        }
    }

    /// Set a header. Multiple calls with the same key append rather
    /// than overwriting; this matches `reqwest::RequestBuilder::header`
    /// semantics and is what callers expect for `Cookie` and
    /// `Set-Cookie`-style multi-valued headers.
    #[must_use]
    pub fn header(mut self, key: &str, value: &str) -> Self {
        match HeaderName::from_bytes(key.as_bytes()) {
            Ok(name) => match HeaderValue::from_str(value) {
                Ok(val) => {
                    self.inner.headers.append(name, val);
                }
                Err(e) => {
                    if self.inner.pending_error.is_none() {
                        self.inner.pending_error = Some(Error::InvalidHeader {
                            message: format!("invalid value for header {key:?}: {e}"),
                            source: Some(Box::new(e)),
                        });
                    }
                }
            },
            Err(e) => {
                if self.inner.pending_error.is_none() {
                    self.inner.pending_error = Some(Error::InvalidHeader {
                        message: format!("invalid header name {key:?}: {e}"),
                        source: Some(Box::new(e)),
                    });
                }
            }
        }
        self
    }

    /// Set the request body to a JSON-serialized value. Encoded with
    /// `serde_json` and sets `Content-Type: application/json`.
    ///
    /// A serialization failure (custom `Serialize` impl returning an
    /// error) is captured on the builder and surfaced from the next
    /// `send` / `send_streaming` call as `Error::EncodeBody`. The
    /// fluent setter does not return `Result` because the protocol
    /// crates compose dozens of these chains; threading a `Result`
    /// through every setter would force a `?` after each call and
    /// hurt readability without catching anything callers cannot
    /// already learn about at send time.
    #[must_use]
    pub fn json<B: Serialize + ?Sized>(mut self, body: &B) -> Self {
        match serde_json::to_vec(body) {
            Ok(v) => {
                self.inner.body = Some(Bytes::from(v));
                let ct = HeaderName::from_static("content-type");
                let val = HeaderValue::from_static("application/json");
                self.inner.headers.insert(ct, val);
            }
            Err(e) => {
                // Keep the first deferred error if one is already
                // present; later failures often mask the root cause.
                if self.inner.pending_error.is_none() {
                    self.inner.pending_error = Some(Error::EncodeBody {
                        message: format!("serde_json::to_vec failed: {e}"),
                        source: Some(Box::new(e)),
                    });
                }
            }
        }
        self
    }

    /// Set the request body to raw bytes.
    #[must_use]
    pub fn body(mut self, body: Bytes) -> Self {
        self.inner.body = Some(body);
        self
    }

    /// Override the per-request cost for the rate-limit governor.
    #[must_use]
    pub fn cost(mut self, cost: u32) -> Self {
        self.inner.cost = Some(cost);
        self
    }

    /// Override the per-request retry policy.
    #[must_use]
    pub fn retry(mut self, policy: RetryPolicy) -> Self {
        self.inner.retry = Some(policy);
        self
    }

    /// Override the per-request timeout.
    #[must_use]
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.inner.timeout = Some(timeout);
        self
    }

    /// Declare whether replaying this request can change server state,
    /// overriding the method-derived default.
    ///
    /// The default is `reqwest::Method::is_idempotent()`, so POST,
    /// PATCH, and extension methods are treated as unsafe to replay and
    /// the transport will not retry them once bytes have gone out. Two
    /// shapes want the override:
    ///
    /// - `idempotent(true)` for a read carried over POST. JMAP's single
    ///   `/jmap/api` endpoint is the standing example: a request whose
    ///   method calls are all `*/get` or `*/query` is safe to replay,
    ///   and saying so restores transport-level retry for it.
    /// - `idempotent(false)` for a GET or PUT the caller knows the
    ///   provider treats as an action rather than a read or an
    ///   absolute-state write.
    ///
    /// This does not affect retries driven by a status RESPONSE (5xx,
    /// 429). A complete response is `Acknowledged` evidence that the
    /// server is reporting it did not do the work, which the shared
    /// error model already treats as replayable regardless of
    /// idempotency. Only the transport-failure path consults this.
    #[must_use]
    pub fn idempotent(mut self, idempotent: bool) -> Self {
        self.inner.idempotent = Some(idempotent);
        self
    }

    /// Disable automatic bearer-token injection for this request.
    /// Existing caller-provided headers are still sent unchanged.
    #[must_use]
    pub fn without_bearer_auth(mut self) -> Self {
        self.inner.bearer_auth = false;
        self
    }

    /// Drive the request to completion with the configured retry
    /// budget, returning the buffered response.
    pub async fn send(self) -> Result<Response, Error> {
        let limit = self.inner.account.net().config().max_buffered_response;
        let internal = send_streaming_inner(self).await?;
        // Drain the body into a single `Bytes`. The retry loop has
        // already validated status; everything from here is a
        // straight body read. Apply the bandwidth meter to the read
        // so buffered receives feed the same counters and cap throttle
        // as streaming.
        let mut body_stream = wrap_metered(internal.body, internal.account);
        let mut accum: Vec<u8> = Vec::new();
        use futures::StreamExt;
        while let Some(chunk) = body_stream.next().await {
            let chunk = chunk?;
            // Checked before the extend, so the ceiling bounds what is
            // actually held rather than being noticed one chunk after
            // the allocation that mattered. The stream drops here, so a
            // provider streaming gigabytes stops being read.
            if let Some(limit) = limit
                && accum.len() + chunk.len() > limit
            {
                return Err(Error::ResponseTooLarge { limit });
            }
            accum.extend_from_slice(&chunk);
        }
        Ok(Response {
            status: internal.status,
            headers: internal.headers,
            body: Bytes::from(accum),
        })
    }

    /// Drive the request to completion but return the response body
    /// as a `ByteStream` rather than buffering. Used for blob
    /// download endpoints.
    pub async fn send_streaming(self) -> Result<StreamingResponse, Error> {
        let internal = send_streaming_inner(self).await?;
        // Caller-facing streaming response wraps the body in the
        // bandwidth meter + cap adapter.
        let metered = wrap_metered(internal.body, internal.account);
        Ok(StreamingResponse {
            status: internal.status,
            headers: internal.headers,
            body: metered,
        })
    }
}

/// Buffered HTTP response.
#[non_exhaustive]
pub struct Response {
    /// HTTP status code of the final attempt.
    pub status: StatusCode,
    /// Response headers as received.
    pub headers: HeaderMap,
    /// Buffered response body.
    pub body: Bytes,
}

impl Response {
    /// HTTP status code of the final attempt.
    #[must_use]
    pub fn status(&self) -> StatusCode {
        self.status
    }

    /// Response headers as received.
    #[must_use]
    pub fn headers(&self) -> &HeaderMap {
        &self.headers
    }
}

/// Streaming HTTP response. The body is a `ByteStream` so the caller
/// can apply backpressure and avoid buffering large attachments.
#[non_exhaustive]
pub struct StreamingResponse {
    /// HTTP status code of the final attempt.
    pub status: StatusCode,
    /// Response headers as received.
    pub headers: HeaderMap,
    /// Response body as an erased byte stream. Increments the
    /// bandwidth meter on every chunk.
    pub body: ByteStream,
}

impl StreamingResponse {
    /// HTTP status code of the final attempt.
    #[must_use]
    pub fn status(&self) -> StatusCode {
        self.status
    }

    /// Response headers as received.
    #[must_use]
    pub fn headers(&self) -> &HeaderMap {
        &self.headers
    }
}

/// Internal streaming response carrying the originating `AccountNet`
/// so `send()` and `send_streaming()` can share one underlying call
/// site without re-binding the body. `body` is unmetered here; the
/// outer wrappers attach the meter at the point of public exposure.
pub(crate) struct InternalStreaming {
    pub(crate) status: StatusCode,
    pub(crate) headers: HeaderMap,
    pub(crate) body: ByteStream,
    pub(crate) account: AccountNet,
}

/// Drive a request to a streamable response, running the retry loop
/// against the configured policy. The body is exposed unmetered;
/// outer wrappers add metering. Used by `RequestBuilder::send` (which
/// drains the body afterwards) and `RequestBuilder::send_streaming`
/// (which exposes the stream).
///
/// The function also walks 3xx redirect chains under the configured
/// `FollowRedirects` policy. Redirect classification calls
/// `redirect::classify_redirect` which enforces RFC 7231 §6.4 method
/// rewriting, the trusted-host allowlist, and `Authorization` stripping
/// on cross-host hops; this function then rebuilds the request from
/// the resulting `RedirectStep` and re-enters the retry loop with the
/// rewritten state. Each redirect step counts as zero retries (it is
/// a fresh logical request) but counts as one hop against
/// `RedirectPolicy::max_hops`.
///
/// Ordering note (accepted): retries run before `AccountError`
/// classification, so intermediate attempts surface only as raw
/// `Error` values and the classified shape reflects the final attempt.
/// The final classification is correct even if the intermediate shape
/// is not pretty - do not re-raise.
pub(crate) async fn send_streaming_inner(
    builder: RequestBuilder,
) -> Result<InternalStreaming, Error> {
    let RequestBuilderInner {
        account,
        method,
        url,
        headers,
        body,
        cost,
        retry,
        timeout,
        idempotent,
        bearer_auth,
        pending_error,
    } = builder.inner;

    // Surface any deferred error from a fluent setter (e.g. `json()`
    // failing to serialize) before touching the network. We do this
    // *before* acquiring a rate-limit slot so a malformed request
    // does not burn anyone else's quota.
    if let Some(err) = pending_error {
        return Err(err);
    }

    let policy = retry.unwrap_or_else(|| account.default_retry().clone());
    let redirect_policy = account.net().config().follow_redirects.clone();
    // Per-request method, URL, body, headers, host, cost. These
    // change across redirect hops: 301/302/303 rewrite to GET and
    // drop the body, 307/308 preserve, and a cross-host hop changes
    // the host bucket the rate-limit governor uses.
    let mut method = method;
    let mut url = url;
    let mut body = body;
    let mut headers = headers;
    let mut auth_for_next_hop = bearer_auth;
    let mut host = host_from_url(&url);
    let mut cost_units = recompute_cost_units(account.net().governor(), host.as_deref(), cost);

    let mut attempt: u32 = 0;
    let mut retry_after_history: Vec<Duration> = Vec::new();
    // Network retry budget is independent from the 401-recovery
    // budget. A 401 forces a token refresh + retry that must not
    // burn the network budget (otherwise a single stale cache hit
    // halves the retries left for transient 5xx). Cap 401 retries at
    // 1; the second 401 returns `Error::AuthLost`.
    let mut auth_retries: u32 = 0;
    const MAX_AUTH_RETRIES: u32 = 1;
    // Redirect hop count. Each redirect hop is a fresh logical
    // request - retry-budget zero, auth-budget zero, but one tick
    // against the configured `RedirectPolicy::max_hops`.
    let mut redirect_hops: u16 = 0;

    'outer: loop {
        attempt = attempt.saturating_add(1);
        // Resolved per attempt rather than once, because a redirect hop
        // can rewrite the method: RFC 7231 §6.4 turns a 301/302/303 on a
        // POST into a GET, and the GET that results is replayable even
        // though the request the caller wrote was not.
        let replayable = idempotent.unwrap_or_else(|| method.is_idempotent());
        // Acquire a rate-limit slot. No-op if no host is configured.
        // Surfaces `Error::CostExceedsBurst` if the caller asked for
        // more units than the host's bucket can ever hold; that is a
        // configuration bug, not a transient condition, so we do not
        // burn retry budget on it.
        if let Some(ref h) = host {
            account.net().governor().acquire(h, cost_units).await?;
        }
        // Until the server acknowledges the request, cancellation or
        // any early return must restore the debited slot. Once a
        // response arrives the guard is disarmed because the server
        // has consumed the attempt. Explicit retry and redirect paths
        // below retain their existing refund rules.
        let mut rate_debit = host
            .as_ref()
            .map(|h| RateDebit::new(account.net().governor().clone(), h.clone(), cost_units));

        // Mint a fresh `Authorization` from the token source. The
        // token source itself handles single-flight refresh.
        //
        // Error classification: `auth.rs::arc_err_to_error` already
        // sorts the underlying failure into the right typed variant -
        // `AuthLost` for terminal auth failures (revoked refresh
        // token, 401/403 from the token endpoint), `RefreshFailed`
        // wrapping a transient `Network`/`Timeout` for everything
        // else. We pass the variant through unchanged so callers can
        // pattern-match for retry-vs-give-up decisions; collapsing
        // every variant into `AuthLost` would discard that signal.
        let token = if auth_for_next_hop {
            let source = account
                .token_source()
                .ok_or_else(|| Error::InvalidRequest {
                    field: "bearer_auth",
                    detail: "bearer authentication requires an AccountSpec token source".to_owned(),
                })?;
            match source.current().await {
                Ok(t) => Some(t),
                Err(e) => return Err(e),
            }
        } else {
            None
        };

        // Record outbound body bytes against the per-account meter
        // before we issue the request. Each retry is another wire
        // transmission, so we count the body once per attempt. We do
        // this even on 401-recovery retries because the failed
        // attempt did put bytes on the wire. Header bytes are
        // intentionally excluded - the meter is a payload sizing
        // tool, not a TCP byte counter.
        if let Some(ref b) = body {
            let out_n = u64::try_from(b.len()).unwrap_or(u64::MAX);
            account.meter().record_bytes_out(out_n);
        }

        let request = build_reqwest(
            account.net().client(),
            &method,
            &url,
            &headers,
            &body,
            token.as_ref(),
            timeout,
        );

        let response = match account.net().dispatch().send(request).await {
            Ok(r) => {
                if let Some(debit) = rate_debit.as_mut() {
                    debit.disarm();
                }
                r
            }
            Err(e) => {
                if policy.network_errors
                    && attempt < policy.max_attempts
                    && transport_error_is_replayable(&e, replayable)
                {
                    let delay = backoff_for(&policy, attempt);
                    tokio::time::sleep(delay).await;
                    continue;
                }
                return Err(e);
            }
        };

        let status = response.status();

        // 401 path. We allow exactly one forced-refresh + retry per
        // request. If the refresh itself fails we surface AuthLost
        // immediately - retrying the same stale token would just
        // produce another 401. If the refresh succeeds and the
        // server still returns 401, the credential is dead; surface
        // AuthLost rather than `Error::Status { code: 401 }` so the
        // protocol crate can map straight to the terminal-auth
        // recovery class. We also refund the rate-limit slot so the
        // retry does not double-debit the host bucket. The auth
        // retry budget (`auth_retries`) is separate from the network
        // retry budget (`attempt`): the retry loop's top
        // `attempt = attempt + 1` increment is undone here so a 401
        // recovery does not eat into the network attempts left.
        if auth_for_next_hop && status == StatusCode::UNAUTHORIZED {
            if auth_retries >= MAX_AUTH_RETRIES {
                let final_response = final_response_from_response(response).await;
                return Err(Error::AuthLost {
                    transmission_state: Some(TransmissionState::Acknowledged),
                    final_response: Some(final_response),
                });
            }
            auth_retries = auth_retries.saturating_add(1);
            drop(response);
            if let Some(ref h) = host {
                account.net().governor().refund(h, cost_units);
            }
            account
                .token_source()
                .expect("bearer-authenticated request validated its token source")
                .refresh()
                .await?;
            // Undo the top-of-loop network-budget increment: 401 is
            // its own one-shot recovery path tracked by
            // `auth_retries`. `continue 'outer` re-enters the loop;
            // the next `attempt = attempt + 1` brings us back to the
            // pre-401 attempt count.
            attempt = attempt.saturating_sub(1);
            continue 'outer;
        }

        // 2xx: return directly. 3xx: classify against the redirect
        // policy. The classifier yields `PassThrough` for non-followed
        // 3xx (304 Not Modified, 305, 306) so conditional-request
        // flows (`If-None-Match` -> 304) and protocol-specific
        // surfaces (Graph's 304 on `$delta`) keep working.
        if status.is_success() {
            let headers_out = response.headers().clone();
            let stream = into_byte_stream(response);
            return Ok(InternalStreaming {
                status,
                headers: headers_out,
                body: stream,
                account,
            });
        }

        if status.is_redirection() {
            let resp_headers = response.headers().clone();
            // Do not refund the rate-limit slot at this point: a 3xx
            // is a real server response. The refund happens only on
            // the `RedirectAction::Follow` path below, where the
            // next hop will issue a fresh request that should debit
            // anew.
            let active_policy: Option<&RedirectPolicy> = match &redirect_policy {
                FollowRedirects::Disabled => None,
                FollowRedirects::Enabled(p) => Some(p),
            };
            match active_policy {
                None => {
                    // Pass 3xx through to the caller exactly as the
                    // original implementation did. Caller code
                    // (conditional GET, 304 handling) reads the
                    // status + headers.
                    let stream = into_byte_stream(response);
                    return Ok(InternalStreaming {
                        status,
                        headers: resp_headers,
                        body: stream,
                        account,
                    });
                }
                Some(policy) => {
                    drop(response);
                    let parsed_url =
                        reqwest::Url::parse(&url).map_err(|e| Error::InvalidRequest {
                            field: "url",
                            detail: format!("could not re-parse request URL for redirect: {e}"),
                        })?;
                    match classify_redirect(policy, &method, &parsed_url, status, &resp_headers)? {
                        RedirectAction::PassThrough => {
                            // Build an empty byte stream so downstream
                            // unwrap paths (e.g. send().drain) still
                            // work. 304/305/306 carry no body the caller
                            // cares about. This arm also covers a
                            // followed-redirect status whose `Location`
                            // is absent (notably Google Drive's `308
                            // Resume Incomplete`): the consumer reads only
                            // the status + headers (`Range`), so the
                            // passed-through body is intentionally
                            // discarded here. If a future passthrough
                            // shape needs its body, classify_redirect must
                            // stop folding it into PassThrough.
                            let empty: futures::stream::Empty<Result<Bytes, Error>> =
                                futures::stream::empty();
                            return Ok(InternalStreaming {
                                status,
                                headers: resp_headers,
                                body: Box::pin(empty),
                                account,
                            });
                        }
                        RedirectAction::Follow(step) => {
                            redirect_hops = redirect_hops.saturating_add(1);
                            if redirect_hops > u16::from(policy.max_hops) {
                                return Err(Error::RedirectLoop {
                                    hops: redirect_hops,
                                });
                            }
                            // The next hop will issue a fresh request
                            // and debit anew, so refund the slot the
                            // 3xx debited. Otherwise a 10-hop chain
                            // would burn 10 units instead of one.
                            if let Some(ref h) = host {
                                account.net().governor().refund(h, cost_units);
                            }
                            method = step.next_method;
                            url = step.next_url;
                            if !step.preserve_body {
                                body = None;
                                // RFC 7231 §6.4 also drops content-
                                // describing headers when the body is
                                // dropped, otherwise the next hop
                                // carries a content-type for a body
                                // that no longer exists.
                                strip_body_headers(&mut headers);
                            }
                            auth_for_next_hop = step.keep_auth && auth_for_next_hop;
                            // Strip caller-set `Authorization` headers
                            // too on cross-host hops, not just the
                            // token-source-derived bearer. Otherwise
                            // JMAP Basic-auth (or any caller that
                            // builds its own Authorization header)
                            // leaks credentials across a host
                            // boundary even when the bearer-injection
                            // path is suppressed.
                            if !step.keep_auth {
                                headers.remove(AUTHORIZATION);
                            }
                            host = host_from_url(&url);
                            cost_units = recompute_cost_units(
                                account.net().governor(),
                                host.as_deref(),
                                cost,
                            );
                            // Reset retry counter for the next hop:
                            // a redirect is a fresh logical request,
                            // its retries should not eat into the
                            // budget of the prior hop.
                            attempt = 0;
                            auth_retries = 0;
                            continue 'outer;
                        }
                    }
                }
            }
        }

        // 4xx that the policy does not call retryable: terminal.
        if status.is_client_error() && !policy.statuses.contains(&status) {
            let headers_out = response.headers().clone();
            let body_bytes = response.bytes().await.unwrap_or_default();
            return Err(Error::Status {
                code: status,
                body: crate::error::cap_status_body(body_bytes),
                headers: headers_out,
            });
        }

        // 5xx and configured-retryable statuses: retry path.
        // `policy.statuses` is the *additive* set (typically just 429
        // and the 5xx codes the default policy lists for clarity),
        // and `is_server_error()` is applied unconditionally on top
        // so 5xx is always retried. Callers that disable retries set
        // `max_attempts = 1` and let the budget-exhausted branch
        // below surface the failure on the first attempt; they do
        // not remove specific codes.
        if policy.statuses.contains(&status) || status.is_server_error() {
            if attempt >= policy.max_attempts {
                // Parse the final attempt's `Retry-After` and push it
                // into the history before we surface the error. The
                // previous code skipped the parse on exhaustion, so
                // callers lost the server's last hint - the exact
                // case where a planner wants to back off the longest.
                let final_retry_after = parse_retry_after(response.headers().get(RETRY_AFTER))
                    .map(|d| d.min(policy.honor_retry_after_cap));
                if let Some(ra) = final_retry_after {
                    retry_after_history.push(ra);
                }
                let final_response = final_response_from_response(response).await;
                if status == StatusCode::TOO_MANY_REQUESTS {
                    let last = retry_after_history.last().copied();
                    return Err(Error::RateLimited {
                        retry_after: last,
                        final_response,
                    });
                }
                return Err(Error::RetryBudgetExhausted {
                    final_response: Some(final_response),
                    retry_after_history,
                });
            }
            // `Retry-After` is capped by `policy.honor_retry_after_cap`,
            // which is the sole source of truth for the cap. Earlier
            // drafts also applied a hardcoded five-minute ceiling; that
            // double-cap is gone so callers that want to honor an
            // hour-long server hint can configure the policy and have
            // it actually take effect.
            let retry_after = parse_retry_after(response.headers().get(RETRY_AFTER))
                .map(|d| d.min(policy.honor_retry_after_cap));
            let wait = retry_after.unwrap_or_else(|| backoff_for(&policy, attempt));
            if let Some(ra) = retry_after {
                retry_after_history.push(ra);
            }
            // Discard the body so the underlying connection can be
            // returned to the pool.
            drop(response);
            // Refund the rate-limit slot on every retried failure: the
            // server did not consume real work on a 5xx or 429, so
            // burning a token across the retry would just starve other
            // waiters on the same host. Previously only 429+
            // `Retry-After` refunded; a plain 503 with no header burned
            // tokens across all three attempts and stalled neighboring
            // requests for the duration of the backoff.
            if let Some(ref h) = host {
                account.net().governor().refund(h, cost_units);
            }
            tokio::time::sleep(wait).await;
            continue;
        }

        // Anything else: surface as Status, no retry.
        let headers_out = response.headers().clone();
        let body_bytes = response.bytes().await.unwrap_or_default();
        return Err(Error::Status {
            code: status,
            body: crate::error::cap_status_body(body_bytes),
            headers: headers_out,
        });
    }
}

/// Build the underlying `reqwest::RequestBuilder` from the captured
/// pieces. Injects `Authorization`, `traceparent`, and the per-request
/// timeout. Body is cloned from `Bytes` so each retry attempt sends
/// the same payload.
fn build_reqwest(
    client: &reqwest::Client,
    method: &reqwest::Method,
    url: &str,
    headers: &HeaderMap,
    body: &Option<Bytes>,
    token: Option<&AccessToken>,
    timeout: Option<Duration>,
) -> reqwest::RequestBuilder {
    let mut req = client.request(method.clone(), url);
    for (k, v) in headers {
        req = req.header(k.clone(), v.clone());
    }
    // Authorization: Bearer <token>. The token may be empty if the
    // protocol crate is on no-auth mode; we still emit the header
    // so call sites don't see undocumented gaps.
    if let Some(token) = token
        && let Ok(val) = HeaderValue::from_str(&format!("Bearer {}", token.as_str()))
    {
        req = req.header(AUTHORIZATION, val);
    }
    // W3C traceparent.
    let tp = crate::trace::current_traceparent();
    if let Ok(val) = HeaderValue::from_str(&tp) {
        req = req.header(HeaderName::from_static("traceparent"), val);
    }
    if let Some(t) = timeout {
        req = req.timeout(t);
    }
    if let Some(b) = body {
        req = req.body(b.clone());
    }
    req
}

/// Whether a transport failure may be replayed on the wire.
///
/// Two conditions make a replay safe, and either suffices: nothing was
/// transmitted, or transmitting it again cannot change server state.
/// Past `Unsent` on a non-idempotent request the side effect may
/// already have landed - `send_error_to_error` classifies every
/// non-connect, non-timeout reqwest failure as
/// `Network { InFlight }`, which is precisely the "may have landed"
/// evidence - and a blind replay is how one Gmail `messages/send`
/// becomes two delivered messages, or one JMAP `Email/set` create
/// becomes two drafts.
///
/// Refusing the replay is not the end of the retry story. The error
/// surfaces to `into_account_error`, which has the caller's real
/// `AccountOperation` and routes `InFlight` + non-idempotent to
/// `Reconcile(TransportDropAfterSend)` and `InFlight` + idempotent to
/// `Retry(SameRequest)`. The decision moves up to the layer that knows
/// what the request meant, instead of being taken three times by a
/// layer that only knows it is holding bytes.
///
/// A variant carrying no transmission evidence is not replayable here:
/// `InvalidRequest` and the rest are local failures a retry cannot fix.
fn transport_error_is_replayable(error: &Error, idempotent: bool) -> bool {
    let state = match error {
        Error::Network {
            transmission_state, ..
        }
        | Error::Tls {
            transmission_state, ..
        }
        | Error::Timeout { transmission_state } => transmission_state,
        _ => return false,
    };
    idempotent || *state == TransmissionState::Unsent
}

pub(crate) fn send_error_to_error(e: reqwest::Error) -> Error {
    let message = format!("{e}");
    if e.is_builder() {
        // Builder failures become `InvalidRequest`; the retry loop's
        // typed retry guard excludes that variant.
        return Error::InvalidRequest {
            field: "request",
            detail: message,
        };
    }
    if native_tls_error_in_source_chain(&e) {
        return Error::Tls {
            message,
            transmission_state: TransmissionState::Unsent,
        };
    }
    if e.is_timeout() && e.is_connect() {
        return Error::Timeout {
            transmission_state: TransmissionState::Unsent,
        };
    }
    if e.is_timeout() {
        return Error::Timeout {
            transmission_state: TransmissionState::InFlight,
        };
    }
    if e.is_connect() {
        return Error::Network {
            message,
            transmission_state: TransmissionState::Unsent,
            source: Some(Box::new(e)),
        };
    }
    Error::Network {
        message,
        transmission_state: TransmissionState::InFlight,
        source: Some(Box::new(e)),
    }
}

fn native_tls_error_in_source_chain(e: &reqwest::Error) -> bool {
    let mut current: Option<&(dyn StdError + 'static)> = Some(e);
    while let Some(err) = current {
        if err.downcast_ref::<native_tls::Error>().is_some() {
            return true;
        }
        current = err.source();
    }
    false
}

async fn final_response_from_response(response: reqwest::Response) -> FinalResponse {
    let status = response.status();
    let headers = response.headers().clone();
    let body = read_capped_response_body(response).await;
    FinalResponse {
        status,
        headers,
        body,
    }
}

async fn read_capped_response_body(response: reqwest::Response) -> Bytes {
    use futures::StreamExt;

    let mut stream = response.bytes_stream();
    let mut buf = Vec::new();
    while let Some(chunk) = stream.next().await {
        let Ok(chunk) = chunk else {
            break;
        };
        if buf.len() <= STATUS_BODY_CAP {
            let remaining = STATUS_BODY_CAP + 1 - buf.len();
            if chunk.len() > remaining {
                buf.extend_from_slice(&chunk[..remaining]);
                break;
            }
            buf.extend_from_slice(&chunk);
        } else {
            break;
        }
    }
    crate::error::cap_status_body(Bytes::from(buf))
}

/// Extract host string from a URL. Returns `None` if the URL is not
/// parseable; the rate-limit governor then no-ops for this request.
fn host_from_url(url: &str) -> Option<String> {
    reqwest::Url::parse(url)
        .ok()
        .and_then(|u| u.host_str().map(str::to_owned))
}

/// Resolve the rate-limit cost units for `host`, honoring the
/// per-request override `cost_override` when present and falling
/// back to the governor's registered host default. Used at request
/// start and recomputed on every redirect hop because a cross-host
/// hop changes which host bucket the request debits against.
fn recompute_cost_units(
    governor: &crate::rate::RateLimitGovernor,
    host: Option<&str>,
    cost_override: Option<u32>,
) -> u32 {
    match cost_override {
        Some(n) => n,
        None => host.and_then(|h| governor.cost_default_for(h)).unwrap_or(1),
    }
}

/// Strip body-describing headers after a redirect demands the body
/// be dropped (RFC 7231 §6.4 method rewrite to GET). Without this,
/// the next hop carries `Content-Type` / `Content-Length` /
/// `Content-Encoding` for a body that no longer exists.
fn strip_body_headers(headers: &mut HeaderMap) {
    headers.remove(reqwest::header::CONTENT_TYPE);
    headers.remove(reqwest::header::CONTENT_LENGTH);
    headers.remove(reqwest::header::CONTENT_ENCODING);
}

/// Exponential backoff with jitter. The base doubles per attempt,
/// capped at `policy.max_backoff`. Decorrelated jitter (half-to-full
/// of the base) prevents thundering-herd retries from one shared
/// outage.
///
/// Jitter is **proportional** to the capped backoff rather than a
/// fixed 0..1 s window. With a short `initial_backoff` (e.g. 10 ms),
/// a fixed-ms jitter would dominate by two orders of magnitude and
/// make the backoff effectively a random-1-second sleep. By taking
/// jitter from `0..capped`, the wait stays in the spirit of the
/// policy: half-base plus up-to-full-base.
fn backoff_for(policy: &RetryPolicy, attempt: u32) -> Duration {
    let base = policy.initial_backoff;
    let exp = attempt.saturating_sub(1).min(16);
    let scaled = base.saturating_mul(2u32.saturating_pow(exp));
    let capped = scaled.min(policy.max_backoff);
    // Use the workspace's UUID RNG as a cheap per-attempt jitter
    // source. This is not cryptographic policy; it just avoids
    // lockstep modulus walks between clients that started together.
    let nanos = uuid::Uuid::new_v4().as_u128();
    let capped_ns = u128::from(u64::try_from(capped.as_nanos()).unwrap_or(u64::MAX)).max(1);
    let jitter_ns = u64::try_from(nanos % capped_ns).unwrap_or(0);
    let half = capped / 2;
    let jitter = Duration::from_nanos(jitter_ns);
    half.saturating_add(jitter).min(policy.max_backoff)
}

/// Parse a `Retry-After` header. Either delta-seconds (an integer) or
/// an HTTP-date per RFC 9110 section 10.2.3.
///
/// Public so protocol crates that read `Retry-After` on responses they
/// handle themselves (without going through `bifrost-net::Error::Status`)
/// share a single parser. The companion shape on `AccountError` is the
/// `retry_after` field on `ServerCause::{Unavailable, RateLimited,
/// QuotaExhausted}` and the `not_before` field on `RetryAdvice`.
pub fn parse_retry_after(value: Option<&HeaderValue>) -> Option<Duration> {
    let v = value?.to_str().ok()?.trim();
    if let Ok(secs) = v.parse::<u64>() {
        return Some(Duration::from_secs(secs));
    }
    let when = httpdate::parse_http_date(v).ok()?;
    let now = std::time::SystemTime::now();
    when.duration_since(now).ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::StaticTokenSource;
    use crate::config::NetConfig;
    use crate::rate::{RateLimit, RateLimitGovernor};
    use crate::redirect::FollowRedirects;
    use crate::test_support::{Canned, ScriptedDispatch, canned, canned_with_headers};
    use reqwest::header::LOCATION;
    use std::sync::Arc;

    // The scripted double these tests drive is the published one in
    // `crate::test_support`, not a private copy - so a downstream crate
    // scripting a status gets the same wire contract this crate pins.
    fn scripted_account(
        script: &Arc<ScriptedDispatch>,
        config: NetConfig,
        hosts: Vec<RateLimit>,
        retry: RetryPolicy,
    ) -> crate::net::AccountNet {
        crate::test_support::scripted_account(
            script,
            config,
            hosts,
            Arc::new(StaticTokenSource::new("token", None)),
            retry,
        )
    }

    fn hv(value: &str) -> HeaderValue {
        HeaderValue::from_str(value).expect("test header value")
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn scripted_retry_refunds_quota_and_preserves_final_response() {
        let mut retry_after = HeaderMap::new();
        retry_after.insert(RETRY_AFTER, HeaderValue::from_static("0"));
        let script = ScriptedDispatch::new([
            canned_with_headers(StatusCode::SERVICE_UNAVAILABLE, retry_after, b"retry"),
            canned(StatusCode::OK, b"ok"),
        ]);
        let retry = RetryPolicy {
            max_attempts: 2,
            initial_backoff: Duration::ZERO,
            max_backoff: Duration::ZERO,
            ..RetryPolicy::default()
        };
        let account = scripted_account(
            &script,
            NetConfig::default(),
            vec![RateLimit {
                host: "retry.test".to_string(),
                quota_per_second: 0.0001,
                cost_default: 1,
                burst: 1,
            }],
            retry,
        );
        let started = tokio::time::Instant::now();

        let response = account
            .post("https://retry.test/resource")
            .body(Bytes::from_static(b"payload"))
            .send()
            .await
            .expect("second scripted attempt succeeds");

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.body, Bytes::from_static(b"ok"));
        assert_eq!(script.requests().len(), 2);
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "the retried 503 must refund the only quota token"
        );
        assert_eq!(account.meter().bytes_out(), 14);
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn scripted_network_failure_retries_through_the_same_loop() {
        let script = ScriptedDispatch::new([
            Canned::Error(Error::Network {
                message: "synthetic reset".to_string(),
                transmission_state: TransmissionState::Unsent,
                source: None,
            }),
            canned(StatusCode::OK, b"recovered"),
        ]);
        let account = scripted_account(
            &script,
            NetConfig::default(),
            Vec::new(),
            RetryPolicy {
                max_attempts: 2,
                initial_backoff: Duration::ZERO,
                max_backoff: Duration::ZERO,
                ..RetryPolicy::default()
            },
        );

        let response = account
            .get("https://network.test/resource")
            .without_bearer_auth()
            .send()
            .await
            .expect("network retry succeeds");

        assert_eq!(response.body, Bytes::from_static(b"recovered"));
        assert_eq!(script.requests().len(), 2);
    }

    fn in_flight_reset() -> Canned {
        Canned::Error(Error::Network {
            message: "connection reset mid-body".to_string(),
            transmission_state: TransmissionState::InFlight,
            source: None,
        })
    }

    fn two_attempts() -> RetryPolicy {
        RetryPolicy {
            max_attempts: 2,
            initial_backoff: Duration::ZERO,
            max_backoff: Duration::ZERO,
            ..RetryPolicy::default()
        }
    }

    /// The duplicate-send bug. `send_error_to_error` classifies every
    /// non-connect, non-timeout reqwest failure as
    /// `Network { InFlight }` - the side effect may already have landed
    /// - and the loop used to replay it anyway, three times, before
    /// `into_account_error` ever ran. Against Gmail's
    /// `/messages/send` that is a duplicate delivered message; against
    /// JMAP's `Email/set` it is a duplicate create.
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn an_in_flight_failure_on_a_post_is_not_replayed() {
        let script = ScriptedDispatch::new([in_flight_reset(), canned(StatusCode::OK, b"second")]);
        let account = scripted_account(&script, NetConfig::default(), Vec::new(), two_attempts());

        let Err(error) = account
            .post("https://send.test/messages/send")
            .body(Bytes::from_static(b"payload"))
            .send()
            .await
        else {
            panic!("a POST that may have landed must not be replayed");
        };

        assert!(matches!(
            error,
            Error::Network {
                transmission_state: TransmissionState::InFlight,
                ..
            }
        ));
        assert_eq!(
            script.requests().len(),
            1,
            "exactly one wire attempt; the reconcile decision belongs to the caller"
        );
    }

    /// `Unsent` is the other half of the rule: nothing reached the
    /// server, so replaying a POST cannot duplicate anything.
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn an_unsent_failure_on_a_post_still_retries() {
        let script = ScriptedDispatch::new([
            Canned::Error(Error::Network {
                message: "connect refused".to_string(),
                transmission_state: TransmissionState::Unsent,
                source: None,
            }),
            canned(StatusCode::OK, b"recovered"),
        ]);
        let account = scripted_account(&script, NetConfig::default(), Vec::new(), two_attempts());

        let response = account
            .post("https://send.test/messages/send")
            .body(Bytes::from_static(b"payload"))
            .send()
            .await
            .expect("nothing was transmitted, so the replay is safe");

        assert_eq!(response.body, Bytes::from_static(b"recovered"));
        assert_eq!(script.requests().len(), 2);
    }

    /// An idempotent method is replayable at any transmission state:
    /// sending the same GET twice cannot change server state.
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn an_in_flight_failure_on_a_get_still_retries() {
        let script =
            ScriptedDispatch::new([in_flight_reset(), canned(StatusCode::OK, b"recovered")]);
        let account = scripted_account(&script, NetConfig::default(), Vec::new(), two_attempts());

        let response = account
            .get("https://read.test/resource")
            .send()
            .await
            .expect("an idempotent request is replayable in any state");

        assert_eq!(response.body, Bytes::from_static(b"recovered"));
        assert_eq!(script.requests().len(), 2);
    }

    /// The escape hatch for a read carried over POST, which is JMAP's
    /// whole API shape.
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn an_explicitly_idempotent_post_retries_in_flight() {
        let script =
            ScriptedDispatch::new([in_flight_reset(), canned(StatusCode::OK, b"recovered")]);
        let account = scripted_account(&script, NetConfig::default(), Vec::new(), two_attempts());

        let response = account
            .post("https://jmap.test/jmap/api")
            .idempotent(true)
            .body(Bytes::from_static(b"{\"methodCalls\":[]}"))
            .send()
            .await
            .expect("a caller that knows the payload is read-only opts back in");

        assert_eq!(response.body, Bytes::from_static(b"recovered"));
        assert_eq!(script.requests().len(), 2);
    }

    /// And the inverse: a caller that knows a GET is really an action.
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn an_explicitly_non_idempotent_get_is_not_replayed() {
        let script = ScriptedDispatch::new([in_flight_reset(), canned(StatusCode::OK, b"second")]);
        let account = scripted_account(&script, NetConfig::default(), Vec::new(), two_attempts());

        let Err(error) = account
            .get("https://legacy.test/trigger-action")
            .idempotent(false)
            .send()
            .await
        else {
            panic!("the override wins over the method default");
        };

        assert!(matches!(error, Error::Network { .. }));
        assert_eq!(script.requests().len(), 1);
    }

    /// A status RESPONSE is `Acknowledged` evidence that the server is
    /// reporting it did not do the work, which the shared error model
    /// treats as replayable regardless of idempotency. Idempotency must
    /// not leak into that branch and turn every 503 on a POST into an
    /// immediate failure.
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn a_503_on_a_post_still_retries() {
        let script = ScriptedDispatch::new([
            canned(StatusCode::SERVICE_UNAVAILABLE, b"try again"),
            canned(StatusCode::OK, b"recovered"),
        ]);
        let account = scripted_account(&script, NetConfig::default(), Vec::new(), two_attempts());

        let response = account
            .post("https://send.test/messages/send")
            .body(Bytes::from_static(b"payload"))
            .send()
            .await
            .expect("status-driven retry is unchanged by the idempotency rule");

        assert_eq!(response.body, Bytes::from_static(b"recovered"));
        assert_eq!(script.requests().len(), 2);
    }

    /// `send` buffers the whole body, and every JSON API call in
    /// google / graph / jmap takes that path. Without a ceiling a
    /// provider outage page, a mis-routed blob URL, or a hostile
    /// response OOMs the process. The error path already had a 4 KB
    /// cap; this is the success path's.
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn a_buffered_body_past_the_ceiling_is_refused() {
        let script = ScriptedDispatch::new([canned(StatusCode::OK, &[b'x'; 4096])]);
        let config = NetConfig {
            max_buffered_response: Some(1024),
            ..NetConfig::default()
        };
        let account = scripted_account(&script, config, Vec::new(), RetryPolicy::disabled());

        let Err(error) = account.get("https://big.test/resource").send().await else {
            panic!("a 4 KiB body must not pass a 1 KiB ceiling");
        };

        assert!(
            matches!(error, Error::ResponseTooLarge { limit: 1024 }),
            "unexpected error: {error:?}"
        );
    }

    /// The ceiling must not fire on ordinary traffic, and `None`
    /// disables it entirely.
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn a_body_within_the_ceiling_is_returned_intact() {
        let script = ScriptedDispatch::new([
            canned(StatusCode::OK, b"small"),
            canned(StatusCode::OK, &[b'x'; 4096]),
        ]);
        let account = scripted_account(
            &script,
            NetConfig::default(),
            Vec::new(),
            RetryPolicy::disabled(),
        );
        let response = account
            .get("https://ok.test/resource")
            .send()
            .await
            .expect("a small body is well under the default ceiling");
        assert_eq!(response.body, Bytes::from_static(b"small"));

        let unbounded = scripted_account(
            &script,
            NetConfig {
                max_buffered_response: None,
                ..NetConfig::default()
            },
            Vec::new(),
            RetryPolicy::disabled(),
        );
        let response = unbounded
            .get("https://ok.test/resource")
            .send()
            .await
            .expect("None disables the check");
        assert_eq!(response.body.len(), 4096);
    }

    /// The pipeline supplies no total deadline of its own - the only
    /// per-request timeout on the wire is one the caller asked for.
    ///
    /// `NetConfig::read_timeout` is what bounds a stalled request, and
    /// it is an inactivity bound precisely because it cannot be
    /// overridden: consumers program against `Account` and never reach
    /// the transport config, so a total deadline here would be able to
    /// fail a slow-but-healthy large fetch with no recourse.
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn the_only_request_deadline_is_one_the_caller_set() {
        let script = ScriptedDispatch::new([
            canned(StatusCode::OK, b"one"),
            canned(StatusCode::OK, b"two"),
        ]);
        let account = scripted_account(
            &script,
            NetConfig::default(),
            Vec::new(),
            RetryPolicy::disabled(),
        );

        account
            .get("https://deadline.test/a")
            .send()
            .await
            .expect("first request succeeds");
        account
            .get("https://deadline.test/b")
            .timeout(Duration::from_secs(90))
            .send()
            .await
            .expect("second request succeeds");

        let requests = script.requests();
        assert_eq!(requests.len(), 2);
        assert_eq!(
            requests[0].timeout, None,
            "the pipeline must not invent a total deadline"
        );
        assert_eq!(
            requests[1].timeout,
            Some(Duration::from_secs(90)),
            "a caller-set timeout reaches the wire"
        );
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn scripted_401_refresh_has_a_separate_attempt_budget() {
        let script = ScriptedDispatch::new([
            canned(StatusCode::UNAUTHORIZED, b"stale"),
            canned(StatusCode::OK, b"fresh"),
        ]);
        let account = scripted_account(
            &script,
            NetConfig::default(),
            Vec::new(),
            RetryPolicy {
                max_attempts: 1,
                ..RetryPolicy::default()
            },
        );

        let response = account
            .get("https://auth.test/resource")
            .send()
            .await
            .expect("401 refresh retry does not consume max_attempts");

        assert_eq!(response.body, Bytes::from_static(b"fresh"));
        assert_eq!(script.requests().len(), 2);
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn scripted_second_401_preserves_auth_lost_response() {
        let mut headers = HeaderMap::new();
        headers.insert(
            reqwest::header::WWW_AUTHENTICATE,
            HeaderValue::from_static("Bearer"),
        );
        let script = ScriptedDispatch::new([
            canned(StatusCode::UNAUTHORIZED, b"first"),
            canned_with_headers(StatusCode::UNAUTHORIZED, headers, b"second"),
        ]);
        let account = scripted_account(
            &script,
            NetConfig::default(),
            Vec::new(),
            RetryPolicy::disabled(),
        );

        let error = match account.get("https://auth.test/resource").send().await {
            Err(error) => error,
            Ok(_) => panic!("a second 401 must be terminal auth loss"),
        };

        let Error::AuthLost {
            transmission_state: Some(TransmissionState::Acknowledged),
            final_response: Some(final_response),
        } = error
        else {
            panic!("expected acknowledged AuthLost");
        };
        assert_eq!(final_response.status, StatusCode::UNAUTHORIZED);
        assert_eq!(final_response.body, Bytes::from_static(b"second"));
        assert!(
            final_response
                .headers
                .contains_key(reqwest::header::WWW_AUTHENTICATE)
        );
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn scripted_redirect_rewrites_post_and_strips_cross_host_auth() {
        let mut redirect_headers = HeaderMap::new();
        redirect_headers.insert(
            LOCATION,
            HeaderValue::from_static("https://target.test/final"),
        );
        let script = ScriptedDispatch::new([
            canned_with_headers(StatusCode::FOUND, redirect_headers, b""),
            canned(StatusCode::OK, b"done"),
        ]);
        let account = scripted_account(
            &script,
            NetConfig::default(),
            Vec::new(),
            RetryPolicy::disabled(),
        );

        let response = account
            .post("https://origin.test/start")
            .header(AUTHORIZATION.as_str(), "Basic caller-secret")
            .body(Bytes::from_static(b"body"))
            .send()
            .await
            .expect("redirect target succeeds");

        assert_eq!(response.body, Bytes::from_static(b"done"));
        let requests = script.requests();
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0].method, reqwest::Method::POST);
        assert_eq!(requests[0].body, Some(Bytes::from_static(b"body")));
        assert!(requests[0].headers.contains_key(AUTHORIZATION));
        assert_eq!(requests[1].method, reqwest::Method::GET);
        assert_eq!(requests[1].url.as_str(), "https://target.test/final");
        assert_eq!(requests[1].body, None);
        assert!(!requests[1].headers.contains_key(AUTHORIZATION));
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn max_255_redirect_policy_terminates_on_hop_256() {
        let mut redirect_headers = HeaderMap::new();
        redirect_headers.insert(LOCATION, HeaderValue::from_static("/next"));
        let steps = (0..=255).map(|_| {
            canned_with_headers(
                StatusCode::TEMPORARY_REDIRECT,
                redirect_headers.clone(),
                b"",
            )
        });
        let script = ScriptedDispatch::new(steps);
        let config = NetConfig::default()
            .follow_redirects(FollowRedirects::Enabled(RedirectPolicy::with_hops(255)));
        let account = scripted_account(&script, config, Vec::new(), RetryPolicy::disabled());

        let error = match account
            .get("https://redirect.test/start")
            .without_bearer_auth()
            .send()
            .await
        {
            Err(error) => error,
            Ok(_) => panic!("the widened counter must terminate after the configured maximum"),
        };

        assert!(matches!(error, Error::RedirectLoop { hops: 256 }));
        assert_eq!(script.requests().len(), 256);
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn exhausted_status_retry_keeps_the_final_hint_and_body() {
        let mut first_headers = HeaderMap::new();
        first_headers.insert(RETRY_AFTER, HeaderValue::from_static("1"));
        let mut final_headers = HeaderMap::new();
        final_headers.insert(RETRY_AFTER, HeaderValue::from_static("2"));
        let script = ScriptedDispatch::new([
            canned_with_headers(StatusCode::SERVICE_UNAVAILABLE, first_headers, b"first"),
            canned_with_headers(StatusCode::SERVICE_UNAVAILABLE, final_headers, b"final"),
        ]);
        let account = scripted_account(
            &script,
            NetConfig::default(),
            Vec::new(),
            RetryPolicy {
                max_attempts: 2,
                honor_retry_after_cap: Duration::from_secs(10),
                ..RetryPolicy::default()
            },
        );

        let error = match account
            .get("https://retry.test/resource")
            .without_bearer_auth()
            .send()
            .await
        {
            Err(error) => error,
            Ok(_) => panic!("retry budget must be exhausted"),
        };

        let Error::RetryBudgetExhausted {
            final_response: Some(final_response),
            retry_after_history,
        } = error
        else {
            panic!("expected retry budget evidence");
        };
        assert_eq!(final_response.status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(final_response.body, Bytes::from_static(b"final"));
        assert_eq!(
            retry_after_history,
            vec![Duration::from_secs(1), Duration::from_secs(2)]
        );
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn scripted_download_accepts_a_legal_shortened_closed_range() {
        use futures::TryStreamExt;

        let mut headers = HeaderMap::new();
        headers.insert(
            reqwest::header::CONTENT_RANGE,
            HeaderValue::from_static("bytes 0-49/50"),
        );
        let script = ScriptedDispatch::new([Canned::Response {
            status: StatusCode::PARTIAL_CONTENT,
            headers,
            body: Bytes::from(vec![7; 50]),
        }]);
        let account = scripted_account(
            &script,
            NetConfig::default(),
            Vec::new(),
            RetryPolicy::disabled(),
        );

        let chunks = account
            .download_stream(
                "https://range.test/blob",
                Some(bifrost_types::ByteRange {
                    start: 0,
                    length: Some(100),
                }),
            )
            .await
            .expect("the server returned the complete shorter resource tail")
            .try_collect::<Vec<_>>()
            .await
            .expect("body stream succeeds");

        assert_eq!(chunks.concat(), vec![7; 50]);
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn dropping_an_armed_rate_debit_refunds_the_slot() {
        let governor = RateLimitGovernor::new();
        governor.register(RateLimit {
            host: "cancel.test".to_string(),
            quota_per_second: 0.0001,
            cost_default: 1,
            burst: 1,
        });
        governor
            .acquire("cancel.test", 1)
            .await
            .expect("initial debit");
        drop(RateDebit::new(
            governor.clone(),
            "cancel.test".to_string(),
            1,
        ));

        governor
            .acquire("cancel.test", 1)
            .await
            .expect("guard drop restored the slot");
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn cancelling_an_in_flight_request_refunds_its_rate_debit() {
        let script = ScriptedDispatch::new([Canned::Pending]);
        let account = scripted_account(
            &script,
            NetConfig::default(),
            vec![RateLimit {
                host: "cancel.test".to_string(),
                quota_per_second: 0.0001,
                cost_default: 1,
                burst: 1,
            }],
            RetryPolicy::disabled(),
        );
        let request_account = account.clone();
        let request = tokio::spawn(async move {
            request_account
                .get("https://cancel.test/resource")
                .without_bearer_auth()
                .send()
                .await
        });
        for _ in 0..100 {
            if !script.requests().is_empty() {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert!(
            !script.requests().is_empty(),
            "request never reached the scripted dispatcher"
        );

        request.abort();
        let _ = request.await;

        account
            .net()
            .governor()
            .acquire("cancel.test", 1)
            .await
            .expect("cancelling before a response restores the only token");
    }

    // ---- backoff_for -------------------------------------------------

    /// The documented shape is `half-of-capped` plus proportional
    /// jitter in `0..capped`. Pin both ends of the interval across
    /// enough draws that a broken jitter source shows up.
    #[test]
    fn backoff_first_attempt_stays_inside_the_proportional_window() {
        let policy = RetryPolicy::default();
        for _ in 0..64 {
            let wait = backoff_for(&policy, 1);
            assert!(
                wait >= Duration::from_millis(500),
                "backoff must never dip below half the base, got {wait:?}"
            );
            assert!(
                wait < Duration::from_millis(1500),
                "backoff must stay under half-base plus one full base, got {wait:?}"
            );
        }
    }

    /// The base doubles per attempt until `max_backoff` clamps it.
    /// Attempt 7 with a 1 s base scales to 64 s, which the default
    /// 60 s ceiling caps, so the window becomes `[30 s, 60 s]`.
    #[test]
    fn backoff_growth_is_clamped_by_max_backoff() {
        let policy = RetryPolicy::default();
        for _ in 0..32 {
            let wait = backoff_for(&policy, 7);
            assert!(wait >= Duration::from_secs(30), "got {wait:?}");
            assert!(wait <= policy.max_backoff, "got {wait:?}");
        }
    }

    /// `exp` is clamped at 16 and the result is `min`-ed against
    /// `max_backoff`, so an absurd attempt count cannot produce an
    /// unbounded (or overflowing) sleep.
    #[test]
    fn backoff_saturates_rather_than_overflowing_on_huge_attempt_counts() {
        let policy = RetryPolicy::default();
        for attempt in [17u32, 64, 1_000, u32::MAX] {
            let wait = backoff_for(&policy, attempt);
            assert!(
                wait <= policy.max_backoff,
                "attempt {attempt} produced {wait:?}, above max_backoff"
            );
        }
    }

    /// A degenerate all-zero policy must return `Duration::ZERO`
    /// rather than panic inside the modulus (the `.max(1)` guard on
    /// `capped_ns` is what makes this safe).
    #[test]
    fn backoff_with_zero_durations_is_zero_and_does_not_panic() {
        let policy = RetryPolicy {
            initial_backoff: Duration::ZERO,
            max_backoff: Duration::ZERO,
            ..RetryPolicy::default()
        };
        assert_eq!(backoff_for(&policy, 1), Duration::ZERO);
        assert_eq!(backoff_for(&policy, 9), Duration::ZERO);
    }

    /// `Duration::MAX` inputs exercise the `u64::try_from(as_nanos)`
    /// fallback and the `saturating_add`. The value is nonsense as a
    /// policy but must not panic; it saturates at `max_backoff`.
    #[test]
    fn backoff_with_saturated_durations_does_not_panic() {
        let policy = RetryPolicy {
            initial_backoff: Duration::MAX,
            max_backoff: Duration::MAX,
            ..RetryPolicy::default()
        };
        let wait = backoff_for(&policy, 3);
        assert!(
            wait >= Duration::from_secs(1),
            "a saturated policy still yields a positive wait, got {wait:?}"
        );
    }

    /// A `max_backoff` shorter than `initial_backoff` clamps on the
    /// very first attempt; the window is then `[max/2, max]`.
    #[test]
    fn backoff_honors_a_max_below_the_initial_base() {
        let policy = RetryPolicy {
            initial_backoff: Duration::from_secs(30),
            max_backoff: Duration::from_millis(200),
            ..RetryPolicy::default()
        };
        for _ in 0..32 {
            let wait = backoff_for(&policy, 1);
            assert!(wait >= Duration::from_millis(100), "got {wait:?}");
            assert!(wait <= Duration::from_millis(200), "got {wait:?}");
        }
    }

    // ---- parse_retry_after -------------------------------------------

    #[test]
    fn retry_after_absent_header_is_none() {
        assert_eq!(parse_retry_after(None), None);
    }

    #[test]
    fn retry_after_parses_delta_seconds() {
        assert_eq!(
            parse_retry_after(Some(&hv("30"))),
            Some(Duration::from_secs(30))
        );
        assert_eq!(
            parse_retry_after(Some(&hv("0"))),
            Some(Duration::ZERO),
            "a zero hint is a real instruction, not an absent one"
        );
    }

    #[test]
    fn retry_after_trims_surrounding_whitespace() {
        assert_eq!(
            parse_retry_after(Some(&hv("  45  "))),
            Some(Duration::from_secs(45))
        );
    }

    #[test]
    fn retry_after_rejects_negative_and_garbage() {
        assert_eq!(parse_retry_after(Some(&hv("-5"))), None);
        assert_eq!(parse_retry_after(Some(&hv("soon"))), None);
        assert_eq!(parse_retry_after(Some(&hv(""))), None);
        assert_eq!(parse_retry_after(Some(&hv("1.5"))), None);
    }

    /// An HTTP-date already in the past yields `None` (the header
    /// carries no useful wait), and a future date yields the remaining
    /// delta.
    #[test]
    fn retry_after_parses_http_dates_relative_to_now() {
        assert_eq!(
            parse_retry_after(Some(&hv("Wed, 21 Oct 2015 07:28:00 GMT"))),
            None,
            "a date in the past is not a wait"
        );
        let future =
            httpdate::fmt_http_date(std::time::SystemTime::now() + Duration::from_secs(3600));
        let parsed = parse_retry_after(Some(&hv(&future))).expect("future date parses");
        assert!(parsed <= Duration::from_secs(3600));
        assert!(parsed > Duration::from_secs(3000));
    }

    /// The parser deliberately does NOT cap: an absurd server hint is
    /// returned verbatim and the retry loop applies
    /// `policy.honor_retry_after_cap` as the single capping site. This
    /// pins that the cap lives with the caller, not here.
    #[test]
    fn retry_after_does_not_cap_absurd_server_hints() {
        assert_eq!(
            parse_retry_after(Some(&hv("31536000"))),
            Some(Duration::from_secs(31_536_000)),
            "a one-year hint survives the parser; capping is the policy's job"
        );
        let capped = parse_retry_after(Some(&hv("31536000")))
            .map(|d| d.min(RetryPolicy::default().honor_retry_after_cap));
        assert_eq!(capped, Some(Duration::from_secs(60)));
    }

    // ---- cost resolution ---------------------------------------------

    #[test]
    fn cost_precedence_is_override_then_host_default_then_one() {
        let governor = RateLimitGovernor::new();
        governor.register(RateLimit {
            host: "cost.test".to_owned(),
            quota_per_second: 10.0,
            cost_default: 5,
            burst: 50,
        });

        assert_eq!(
            recompute_cost_units(&governor, Some("cost.test"), Some(9)),
            9,
            "an explicit .cost(n) wins over the host default"
        );
        assert_eq!(
            recompute_cost_units(&governor, Some("cost.test"), None),
            5,
            "without an override the host's registered default applies"
        );
        assert_eq!(
            recompute_cost_units(&governor, Some("unknown.test"), None),
            1,
            "an unregistered host falls back to one unit"
        );
        assert_eq!(
            recompute_cost_units(&governor, None, None),
            1,
            "an unparseable URL has no host bucket and costs one unit"
        );
    }

    // ---- URL / header helpers ----------------------------------------

    #[test]
    fn host_extraction_handles_ports_userinfo_and_junk() {
        assert_eq!(
            host_from_url("https://a.example/path?q=1"),
            Some("a.example".to_owned())
        );
        assert_eq!(
            host_from_url("https://user:pw@b.example:8443/p"),
            Some("b.example".to_owned()),
            "the bucket key is the host alone, without userinfo or port"
        );
        assert_eq!(host_from_url("not a url"), None);
        assert_eq!(
            host_from_url("https://A.EXAMPLE/"),
            Some("a.example".to_owned()),
            "reqwest lowercases the host, so bucket keys are case-normalised"
        );
    }

    #[test]
    fn body_headers_are_stripped_but_others_survive() {
        let mut headers = HeaderMap::new();
        headers.insert(
            reqwest::header::CONTENT_TYPE,
            HeaderValue::from_static("application/json"),
        );
        headers.insert(
            reqwest::header::CONTENT_LENGTH,
            HeaderValue::from_static("42"),
        );
        headers.insert(
            reqwest::header::CONTENT_ENCODING,
            HeaderValue::from_static("gzip"),
        );
        headers.insert(
            HeaderName::from_static("x-keep"),
            HeaderValue::from_static("yes"),
        );
        headers.insert(AUTHORIZATION, HeaderValue::from_static("Bearer t"));

        strip_body_headers(&mut headers);

        assert!(!headers.contains_key(reqwest::header::CONTENT_TYPE));
        assert!(!headers.contains_key(reqwest::header::CONTENT_LENGTH));
        assert!(!headers.contains_key(reqwest::header::CONTENT_ENCODING));
        assert!(
            headers.contains_key(HeaderName::from_static("x-keep")),
            "only body-describing headers are dropped on a method rewrite"
        );
        assert!(
            headers.contains_key(AUTHORIZATION),
            "Authorization is the redirect loop's business, not the body strip's"
        );
    }

    // ---- deferred builder errors -------------------------------------

    /// `RequestBuilder::header` defers a rejected header name or value
    /// to send time rather than returning `Result` from the setter.
    /// Pin the underlying reqwest rejections the deferred-error arms
    /// key off, so a future header crate that starts accepting these
    /// silently does not turn the deferral into dead code.
    #[test]
    fn header_rejections_the_deferred_error_arms_rely_on() {
        assert!(
            HeaderName::from_bytes(b"bad header").is_err(),
            "a space in a header name must be rejected"
        );
        assert!(
            HeaderValue::from_str("bad\nvalue").is_err(),
            "a newline in a header value must be rejected"
        );
        assert!(
            HeaderValue::from_str("fine").is_ok(),
            "an ordinary value must still be accepted"
        );
    }
}
