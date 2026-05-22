//! Per-request builder, finished response shapes, and the streaming
//! body adapter.
//!
//! Protocol crates never see a `reqwest::RequestBuilder` directly.
//! Everything routes through this wrapper so the underlying HTTP
//! stack can be swapped without touching call sites.

use std::pin::Pin;
use std::time::Duration;

use bytes::Bytes;
use futures::Stream;
use reqwest::{
    StatusCode,
    header::{AUTHORIZATION, HeaderMap, HeaderName, HeaderValue, RETRY_AFTER},
};
use serde::Serialize;

use crate::auth::AccessToken;
use crate::error::Error;
use crate::net::{AccountNet, into_byte_stream, wrap_metered};
use crate::redirect::{FollowRedirects, RedirectAction, RedirectPolicy, classify_redirect};
use crate::retry::RetryPolicy;

/// Erased byte-chunk stream, as returned by `AccountNet::download_stream`.
/// One element per chunk reqwest yields off the underlying socket;
/// bandwidth metering wraps every chunk.
pub type ByteStream = Pin<Box<dyn Stream<Item = Result<Bytes, Error>> + Send + 'static>>;

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
    let mut redirect_hops: u8 = 0;

    'outer: loop {
        attempt = attempt.saturating_add(1);
        // Acquire a rate-limit slot. No-op if no host is configured.
        // Surfaces `Error::CostExceedsBurst` if the caller asked for
        // more units than the host's bucket can ever hold; that is a
        // configuration bug, not a transient condition, so we do not
        // burn retry budget on it.
        if let Some(ref h) = host {
            account.net().governor().acquire(h, cost_units).await?;
        }

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
            match account.token_source().current().await {
                Ok(t) => Some(t),
                Err(e) => {
                    // The request never reached the wire; refund the
                    // rate-limit slot so neighbours aren't starved by
                    // a bookkeeping leak.
                    if let Some(ref h) = host {
                        account.net().governor().refund(h, cost_units);
                    }
                    return Err(e);
                }
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

        let response = match request.send().await {
            Ok(r) => r,
            Err(e) => {
                if let Some(ref h) = host {
                    account.net().governor().refund(h, cost_units);
                }
                if e.is_timeout() {
                    if policy.network_errors && attempt < policy.max_attempts {
                        let delay = backoff_for(&policy, attempt);
                        tokio::time::sleep(delay).await;
                        continue;
                    }
                    return Err(Error::Timeout);
                }
                // Connect, body, decode failures are network-level.
                // Retry per policy if `network_errors` is set.
                let msg = format!("{e}");
                if policy.network_errors && attempt < policy.max_attempts {
                    let delay = backoff_for(&policy, attempt);
                    tokio::time::sleep(delay).await;
                    continue;
                }
                return Err(Error::Network {
                    message: msg,
                    source: Some(Box::new(e)),
                });
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
                drop(response);
                return Err(Error::AuthLost);
            }
            auth_retries = auth_retries.saturating_add(1);
            drop(response);
            if let Some(ref h) = host {
                account.net().governor().refund(h, cost_units);
            }
            account.token_source().refresh().await?;
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
                    let parsed_url = reqwest::Url::parse(&url).map_err(|e| Error::Network {
                        message: format!("could not re-parse request URL for redirect: {e}"),
                        source: Some(Box::new(e)),
                    })?;
                    match classify_redirect(policy, &method, &parsed_url, status, &resp_headers)? {
                        RedirectAction::PassThrough => {
                            // Build an empty byte stream so downstream
                            // unwrap paths (e.g. send().drain) still
                            // work; 304/305/306 carry no body the
                            // caller cares about.
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
                            if redirect_hops > policy.max_hops {
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
                // Drop the response body so the connection can return
                // to the pool. The body content is not surfaced in
                // either `RateLimited` or `RetryBudgetExhausted`.
                drop(response);
                if status == StatusCode::TOO_MANY_REQUESTS {
                    let last = retry_after_history.last().copied();
                    return Err(Error::RateLimited { retry_after: last });
                }
                return Err(Error::RetryBudgetExhausted {
                    last_status: Some(status),
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
fn parse_retry_after(value: Option<&HeaderValue>) -> Option<Duration> {
    let v = value?.to_str().ok()?.trim();
    if let Ok(secs) = v.parse::<u64>() {
        return Some(Duration::from_secs(secs));
    }
    let when = httpdate::parse_http_date(v).ok()?;
    let now = std::time::SystemTime::now();
    when.duration_since(now).ok()
}
