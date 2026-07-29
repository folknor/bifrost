//! OAuth bearer-token plumbing with single-flight refresh.
//!
//! The token source is a trait so different OAuth flows (refresh
//! token, device code, service-account JWT) can share one bearer
//! pipeline. `OAuthRefresher` coordinates concurrent refresh
//! attempts behind one `tokio::sync::Mutex` so N in-flight requests
//! do not trigger N independent refreshes.

use std::fmt;
use std::sync::Arc;
use std::sync::RwLock;
use std::time::{Duration, Instant, SystemTime};

use bifrost_types::AccountFuture;
use reqwest::header::RETRY_AFTER;
use tokio::sync::{Mutex, oneshot};
use zeroize::Zeroizing;

use crate::error::{Error, FinalResponse};
use crate::request::parse_retry_after;

/// Default age at which a token without an `expires_at` is treated as
/// stale enough to refresh. Typical OAuth access tokens have a 60-min
/// TTL; refreshing at 55 min leaves a 5-min safety margin without
/// burning the issuer's quota on every request. Wired through
/// `OAuthRefresher::with_max_age` so callers can lengthen or shorten
/// it.
pub const DEFAULT_TOKEN_MAX_AGE: Duration = Duration::from_secs(55 * 60);
const PROACTIVE_REFRESH_RETRY_DELAY: Duration = Duration::from_secs(30);

/// Source of OAuth bearer tokens. Implementations are responsible for
/// holding the refresh token (or whatever provider-specific material
/// is needed) and exchanging it for a fresh access token.
///
/// `current()` returns the cached token; `refresh()` forces a network
/// round-trip and returns the freshly minted token.
pub trait TokenSource: Send + Sync + 'static {
    /// Return the currently cached access token. Implementations must
    /// be cheap; a hot path through `RequestBuilder::send` may call
    /// this once per request.
    fn current(&self) -> AccountFuture<Result<AccessToken, Error>>;

    /// Force a network round-trip to mint a new access token. Called
    /// by `OAuthRefresher` under its single-flight lock or by the
    /// 401-retry path. Implementations must not coalesce internally;
    /// the refresher handles single-flighting.
    fn refresh(&self) -> AccountFuture<Result<AccessToken, Error>>;
}

/// An OAuth access token. The bytes are held in a `Zeroizing<String>`
/// so the buffer is wiped on drop. The `Debug` impl redacts the bytes;
/// only the expiry hint and a fingerprint length are surfaced.
pub struct AccessToken {
    /// Opaque bearer-token bytes. Wrapped in `Zeroizing` to scrub the
    /// allocation on drop. Access goes through `as_str`.
    secret: Zeroizing<String>,
    /// Absolute deadline at which the server is expected to reject
    /// the token. `None` for opaque tokens without a TTL hint, in
    /// which case the refresher falls back to the 401 path.
    expires_at: Option<Instant>,
}

/// Mutable in-memory token source for clients whose caller supplies
/// already-minted access tokens. `refresh()` returns the current token
/// because there is no refresh material inside bifrost-net for this
/// shape.
#[derive(Debug, Clone)]
pub struct StaticTokenSource {
    token: Arc<RwLock<AccessToken>>,
}

impl StaticTokenSource {
    /// Build a static source from raw bearer bytes and an optional
    /// expiry deadline.
    #[must_use]
    pub fn new(secret: impl Into<String>, expires_at: Option<Instant>) -> Self {
        Self::from_token(AccessToken::new(secret, expires_at))
    }

    /// Build a static source from an existing access-token wrapper.
    #[must_use]
    pub fn from_token(token: AccessToken) -> Self {
        Self {
            token: Arc::new(RwLock::new(token)),
        }
    }

    /// Replace the currently exposed token.
    pub fn set(&self, token: AccessToken) {
        *self.token.write().expect("static token lock poisoned") = token;
    }

    /// Snapshot the current token.
    pub fn token(&self) -> AccessToken {
        self.token
            .read()
            .expect("static token lock poisoned")
            .clone()
    }
}

impl TokenSource for StaticTokenSource {
    fn current(&self) -> AccountFuture<Result<AccessToken, Error>> {
        let token = Arc::clone(&self.token);
        Box::pin(async move { Ok(token.read().expect("static token lock poisoned").clone()) })
    }

    fn refresh(&self) -> AccountFuture<Result<AccessToken, Error>> {
        self.current()
    }
}

impl AccessToken {
    /// Build an `AccessToken` from raw bytes and an optional absolute
    /// expiry deadline.
    #[must_use]
    pub fn new(secret: impl Into<String>, expires_at: Option<Instant>) -> Self {
        Self {
            secret: Zeroizing::new(secret.into()),
            expires_at,
        }
    }

    /// Borrow the bearer token as a string slice. Callers must not
    /// log or persist this value.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.secret
    }

    /// Absolute expiry deadline, if the token issuer supplied a TTL.
    #[must_use]
    pub fn expires_at(&self) -> Option<Instant> {
        self.expires_at
    }
}

impl Clone for AccessToken {
    fn clone(&self) -> Self {
        Self {
            secret: Zeroizing::new((*self.secret).clone()),
            expires_at: self.expires_at,
        }
    }
}

impl fmt::Debug for AccessToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Redact the bearer bytes. Surface only metadata that does
        // not enable replay if the log sink is compromised.
        f.debug_struct("AccessToken")
            .field("secret", &"<redacted>")
            .field("len", &self.secret.len())
            .field("expires_at", &self.expires_at)
            .finish()
    }
}

/// Single-flight access-token cache built on top of a `TokenSource`.
///
/// The state machine: `Fresh { token, refreshed_at }` is the steady
/// state; on either a proactive expiry check or a forced refresh, the
/// state transitions to `Refreshing { waiters }` and exactly one
/// spawned task drives the network call. Concurrent callers register a
/// oneshot receiver in `waiters` and await. The spawned driver is
/// deliberate: if the first caller is cancelled mid-refresh, the state
/// still resolves to `Fresh` or `Empty` instead of stranding future
/// callers behind a stale `Refreshing` entry.
pub struct OAuthRefresher {
    /// Underlying provider hooked up at construction.
    source: Arc<dyn TokenSource>,
    /// Shared state behind the single-flight lock.
    state: Arc<Mutex<RefreshState>>,
    /// Maximum age for a cached token without an `expires_at` hint.
    /// Past this age the refresher proactively refreshes instead of
    /// waiting for a server 401. Defaults to `DEFAULT_TOKEN_MAX_AGE`
    /// (55 min) and is tunable via `with_max_age`.
    max_age: Duration,
}

impl OAuthRefresher {
    /// Build a refresher around the given token source. The initial
    /// state is `Empty` - no cached token, no in-flight refresh. The
    /// first `token()` call transitions to `Refreshing` and drives
    /// the network round-trip itself; concurrent callers join the
    /// `waiters` list. The prior draft initialized to
    /// `Refreshing { waiters: [] }` which left no way to distinguish
    /// "refresh in progress" from "first caller hasn't claimed the
    /// refresher role yet" - a naive `token()` impl would park
    /// callers as waiters forever.
    #[must_use]
    pub fn new(source: Arc<dyn TokenSource>) -> Self {
        Self {
            source,
            state: Arc::new(Mutex::new(RefreshState::Empty)),
            max_age: DEFAULT_TOKEN_MAX_AGE,
        }
    }

    /// Override the proactive-refresh max-age for tokens without an
    /// `expires_at` hint. Defaults to `DEFAULT_TOKEN_MAX_AGE`.
    #[must_use]
    pub fn with_max_age(mut self, max_age: Duration) -> Self {
        self.max_age = max_age;
        self
    }

    /// Current proactive-refresh max-age for tokens without an
    /// `expires_at` hint.
    #[must_use]
    pub fn max_age(&self) -> Duration {
        self.max_age
    }

    /// Return the current access token, refreshing if the cached copy
    /// is within the proactive-refresh window or absent.
    ///
    /// Single-flights concurrent refresh attempts. The first caller
    /// to find the state empty or stale transitions the state to
    /// `Refreshing`, registers itself as a waiter, and spawns the
    /// refresh driver. Concurrent callers register a oneshot receiver
    /// and await.
    pub async fn token(&self) -> Result<AccessToken, Error> {
        self.token_inner(false).await
    }

    /// Force a fresh token, bypassing the proactive-refresh window.
    /// Called by the 401-retry path. Itself single-flighted on the
    /// same lock: a forced refresh while another refresh is in
    /// flight registers as a waiter on the existing refresh rather
    /// than launching a second network round-trip.
    pub async fn force_refresh(&self) -> Result<AccessToken, Error> {
        self.token_inner(true).await
    }

    /// Shared engine driving both `token()` and `force_refresh()`.
    /// The only difference between the two flows is whether a `Fresh`
    /// token within its proactive-refresh window short-circuits;
    /// everything else is identical.
    async fn token_inner(&self, force: bool) -> Result<AccessToken, Error> {
        // Lifetime block: snapshot the state, decide the path, drop
        // the guard before any `.await`.
        let role = {
            let mut state = self.state.lock().await;
            match &mut *state {
                RefreshState::Fresh {
                    token,
                    refreshed_at,
                    refresh_not_before,
                } if !force
                    && (!needs_refresh(token, *refreshed_at, self.max_age)
                        || refresh_is_deferred(token, *refresh_not_before)) =>
                {
                    // Steady state: cached token is fresh enough.
                    return Ok(token.clone());
                }
                RefreshState::Fresh {
                    token,
                    refreshed_at,
                    ..
                } => {
                    // This caller is responsible for spawning the
                    // refresh driver. Register it as the first waiter
                    // before dropping the lock so cancellation of this
                    // future cannot leave the state stuck in
                    // `Refreshing`.
                    let (tx, rx) = oneshot::channel();
                    let fallback = if force {
                        None
                    } else {
                        Some((token.clone(), *refreshed_at))
                    };
                    *state = RefreshState::Refreshing {
                        waiters: vec![tx],
                        fallback,
                    };
                    DriverRole::Driver(rx)
                }
                RefreshState::Empty => {
                    let (tx, rx) = oneshot::channel();
                    *state = RefreshState::Refreshing {
                        waiters: vec![tx],
                        fallback: None,
                    };
                    DriverRole::Driver(rx)
                }
                RefreshState::Refreshing { waiters, .. } => {
                    let (tx, rx) = oneshot::channel();
                    waiters.push(tx);
                    DriverRole::Waiter(rx)
                }
            }
        };

        match role {
            DriverRole::Driver(rx) => {
                let driver = self.clone_handle();
                tokio::spawn(async move {
                    let _ = driver.drive_refresh().await;
                });
                wait_for_refresh(rx).await
            }
            DriverRole::Waiter(rx) => wait_for_refresh(rx).await,
        }
    }

    /// Run the refresh future and fan the result out. Re-acquires
    /// the state lock once the network call completes to install the
    /// new `Fresh` entry and drain any waiters that joined while we
    /// were off the lock.
    async fn drive_refresh(&self) -> Result<AccessToken, Error> {
        let result = self.source.refresh().await;
        let mut state = self.state.lock().await;
        // We were the driver. Whatever state we left behind in
        // `Refreshing` must be replaced; pull the waiters out before
        // installing the new `Fresh` or rolling back to `Empty`.
        let (waiters, fallback) = match std::mem::replace(&mut *state, RefreshState::Empty) {
            RefreshState::Refreshing { waiters, fallback } => (waiters, fallback),
            // The only way this can happen is if a second driver
            // claimed the role concurrently, which the single-flight
            // protocol forbids. Defensive.
            _ => (Vec::new(), None),
        };

        match result {
            Ok(token) => {
                *state = RefreshState::Fresh {
                    token: token.clone(),
                    refreshed_at: Instant::now(),
                    refresh_not_before: None,
                };
                drop(state);
                for waiter in waiters {
                    let _ = waiter.send(Ok(token.clone()));
                }
                Ok(token)
            }
            Err(err) => {
                let can_fallback = !is_terminal_auth_error(&err)
                    && fallback
                        .as_ref()
                        .and_then(|(token, _)| token.expires_at())
                        .is_some_and(|expires_at| Instant::now() < expires_at);
                if can_fallback {
                    let (token, refreshed_at) = fallback.expect("fallback was checked as present");
                    *state = RefreshState::Fresh {
                        token: token.clone(),
                        refreshed_at,
                        refresh_not_before: Some(Instant::now() + PROACTIVE_REFRESH_RETRY_DELAY),
                    };
                    drop(state);
                    for waiter in waiters {
                        let _ = waiter.send(Ok(token.clone()));
                    }
                    return Ok(token);
                }
                // Leave state as `Empty` so the next caller drives a
                // fresh attempt rather than parking on a dead
                // `Refreshing` entry.
                drop(state);
                let shared = Arc::new(err);
                for waiter in waiters {
                    let _ = waiter.send(Err(Arc::clone(&shared)));
                }
                Err(arc_err_to_error(shared))
            }
        }
    }

    /// Underlying token source. Exposed so call sites that already
    /// hold a refresher can fall through to the source for paths
    /// where single-flighting is not appropriate.
    #[must_use]
    pub fn source(&self) -> &Arc<dyn TokenSource> {
        &self.source
    }

    fn clone_handle(&self) -> Self {
        Self {
            source: Arc::clone(&self.source),
            state: Arc::clone(&self.state),
            max_age: self.max_age,
        }
    }

    /// Internal state handle. Crate-public so `Net` and `RequestBuilder`
    /// can drive the state machine when Phase 2 wires the refresh
    /// path. Not yet wired - `token()` / `force_refresh()` are the
    /// only paths that read the state.
    #[allow(dead_code)]
    pub(crate) fn state(&self) -> &Mutex<RefreshState> {
        &self.state
    }
}

// `OAuthRefresher` is itself a `TokenSource` so call sites can
// freely substitute a refresher for a raw token source. `current()`
// returns the cached token (refreshing only if stale); `refresh()`
// forces a network round-trip.
//
// **Do not nest refreshers.** Wrapping `OAuthRefresher` around
// another `OAuthRefresher` makes the outer refresher's
// `force_refresh` (via `refresh()`) bypass the inner state machine
// and call the inner's `force_refresh` directly, which always drives
// a network round-trip and defeats the inner's single-flight. If you
// need composition, wrap the underlying provider, not another
// refresher.
impl TokenSource for OAuthRefresher {
    fn current(&self) -> AccountFuture<Result<AccessToken, Error>> {
        // Clone the source-bearing fields so the returned future is
        // `'static`. The trait return is `Pin<Box<...>>` for dyn
        // safety; we don't capture `&self`.
        let me = OAuthRefresher {
            source: Arc::clone(&self.source),
            state: Arc::clone(&self.state),
            max_age: self.max_age,
        };
        Box::pin(async move { me.token().await })
    }

    fn refresh(&self) -> AccountFuture<Result<AccessToken, Error>> {
        let me = OAuthRefresher {
            source: Arc::clone(&self.source),
            state: Arc::clone(&self.state),
            max_age: self.max_age,
        };
        Box::pin(async move { me.force_refresh().await })
    }
}

/// Outcome of the lock-held inspection inside `token_inner`. Captures
/// whether this caller is the refresh driver or one of the parked
/// waiters; carried out of the `Mutex` guard so we can `await` outside
/// the lock.
enum DriverRole {
    /// This caller transitioned the state to `Refreshing` and must
    /// spawn the refresh driver, then await the same waiter path as
    /// every other caller.
    Driver(oneshot::Receiver<Result<AccessToken, Arc<Error>>>),
    /// Another task is already driving the refresh; this caller is
    /// parked on a oneshot until the driver finishes.
    Waiter(oneshot::Receiver<Result<AccessToken, Arc<Error>>>),
}

async fn wait_for_refresh(
    rx: oneshot::Receiver<Result<AccessToken, Arc<Error>>>,
) -> Result<AccessToken, Error> {
    match rx.await {
        Ok(Ok(t)) => Ok(t),
        Ok(Err(e)) => Err(arc_err_to_error(e)),
        // The driver task dropped without sending. Treat as a terminal
        // refresh failure; the state will already have been reset only
        // if the driver reached `drive_refresh`.
        Err(_) => Err(Error::AuthLost {
            transmission_state: None,
            final_response: None,
        }),
    }
}

/// Has the cached token aged past the proactive-refresh threshold?
///
/// Two branches:
///
/// 1. Issuer supplied an `expires_at`: refresh 60 s before that
///    instant. Standard proactive-refresh window.
/// 2. No `expires_at` (opaque tokens without a TTL hint): refresh
///    when the cached token is older than `max_age`. The previous
///    behaviour treated such tokens as fresh indefinitely and waited
///    for a server 401, which leaked latency into every request that
///    happened to coincide with the server-side expiry.
fn needs_refresh(token: &AccessToken, refreshed_at: Instant, max_age: Duration) -> bool {
    let now = Instant::now();
    if let Some(expires_at) = token.expires_at() {
        let window = Duration::from_secs(60);
        let deadline = expires_at.checked_sub(window).unwrap_or(expires_at);
        return now >= deadline;
    }
    // Opaque token: fall back to the configured max-age.
    now.saturating_duration_since(refreshed_at) >= max_age
}

fn refresh_is_deferred(token: &AccessToken, not_before: Option<Instant>) -> bool {
    let now = Instant::now();
    not_before.is_some_and(|not_before| now < not_before)
        && token
            .expires_at()
            .is_some_and(|expires_at| now < expires_at)
}

/// Convert an `Arc<Error>` (the wrapper that lets us fan one refresh
/// failure out to N waiters) back into a fresh owned `Error`. We
/// cannot move out of the `Arc` because waiters may still hold
/// references; instead we project by variant.
///
/// True auth failures (`AuthLost`) map straight through; the caller's
/// downstream classification stays the same. A `Status` from a token
/// endpoint with a 401 or 403 indicates the refresh token itself was
/// rejected and is also an auth-terminal condition.
///
/// Every other failure (`Network`, `Timeout`, `Tls`, etc.) is
/// transient from the consumer's point of view; collapsing them into
/// `AuthLost` would discard the retry-vs-give-up distinction the
/// engine needs. We preserve them inside
/// `RefreshFailed { retry_after, source }` so the caller can either
/// pattern-match on the inner variant for a retry decision or treat
/// the wrapper as a single "refresh failed" class.
fn arc_err_to_error(err: Arc<Error>) -> Error {
    if let Error::AuthLost { final_response, .. } = err.as_ref() {
        return Error::AuthLost {
            transmission_state: None,
            final_response: final_response.clone(),
        };
    }
    if let Error::Status {
        code,
        body,
        headers,
    } = err.as_ref()
        && (*code == reqwest::StatusCode::UNAUTHORIZED || *code == reqwest::StatusCode::FORBIDDEN)
    {
        return Error::AuthLost {
            transmission_state: None,
            final_response: Some(FinalResponse {
                status: *code,
                headers: headers.clone(),
                body: body.clone(),
            }),
        };
    }
    Error::RefreshFailed {
        retry_after: retry_after_deadline(err.as_ref()),
        source: err,
    }
}

fn is_terminal_auth_error(err: &Error) -> bool {
    matches!(err, Error::AuthLost { .. })
        || matches!(
            err,
            Error::Status { code, .. }
                if *code == reqwest::StatusCode::UNAUTHORIZED
                    || *code == reqwest::StatusCode::FORBIDDEN
        )
}

fn retry_after_deadline(err: &Error) -> Option<SystemTime> {
    let duration = match err {
        Error::RateLimited {
            retry_after,
            final_response,
        } => (*retry_after).or_else(|| parse_retry_after(final_response.headers.get(RETRY_AFTER))),
        Error::Status { code, headers, .. }
            if *code == reqwest::StatusCode::TOO_MANY_REQUESTS
                || *code == reqwest::StatusCode::SERVICE_UNAVAILABLE =>
        {
            parse_retry_after(headers.get(RETRY_AFTER))
        }
        Error::RetryBudgetExhausted {
            final_response: Some(final_response),
            retry_after_history,
        } if final_response.status == reqwest::StatusCode::TOO_MANY_REQUESTS
            || final_response.status == reqwest::StatusCode::SERVICE_UNAVAILABLE =>
        {
            parse_retry_after(final_response.headers.get(RETRY_AFTER))
                .or_else(|| retry_after_history.last().copied())
        }
        Error::RefreshFailed { retry_after, .. } => return *retry_after,
        _ => None,
    }?;
    SystemTime::now().checked_add(duration)
}

/// Internal state of `OAuthRefresher`. Wrapped in a `Mutex` so the
/// `Empty -> Refreshing`, `Fresh -> Refreshing`, and
/// `Refreshing -> Fresh` transitions are atomic.
#[non_exhaustive]
pub enum RefreshState {
    /// Initial state: no cached token, no in-flight refresh. The
    /// first `token()` call transitions to `Refreshing` with the
    /// refreshing role implicitly claimed (`waiters` empty); it
    /// drives the network round-trip itself.
    Empty,
    /// Steady state. The cached token is valid and outside the
    /// proactive-refresh window.
    Fresh {
        /// Currently cached token.
        token: AccessToken,
        /// Wall-clock instant at which the cached token was minted.
        refreshed_at: Instant,
        /// Earliest next proactive refresh attempt after a transient
        /// failure. Forced refreshes ignore this deadline.
        refresh_not_before: Option<Instant>,
    },
    /// A refresh is in flight. The task that transitioned the state
    /// to `Refreshing` is driving the network call; concurrent
    /// callers append a oneshot sender to `waiters` and await. On
    /// completion the driving task fans the result out to every
    /// waiter.
    ///
    /// Errors fan out via `Arc<Error>` because `crate::error::Error`
    /// is not `Clone` (it carries `Box<dyn std::error::Error>` and
    /// `Bytes` and a `HeaderMap`). Each waiter receives the same
    /// `Arc<Error>`; cloning the `Arc` is one refcount bump.
    Refreshing {
        /// Pending oneshot senders, one per waiting caller. The
        /// driving task is NOT in this list - it owns the refresh
        /// future directly.
        waiters: Vec<oneshot::Sender<Result<AccessToken, Arc<Error>>>>,
        /// Still-valid cached token displaced by a proactive refresh.
        /// A transient refresh failure restores this entry so the
        /// caller can use the remaining token lifetime. Forced
        /// refreshes never carry a fallback because a target 401 is
        /// evidence that the cached credential is unusable.
        fallback: Option<(AccessToken, Instant)>,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use reqwest::header::{HeaderMap, HeaderValue, WWW_AUTHENTICATE};
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct FailingRefreshSource {
        calls: AtomicUsize,
    }

    impl TokenSource for FailingRefreshSource {
        fn current(&self) -> AccountFuture<Result<AccessToken, Error>> {
            self.refresh()
        }

        fn refresh(&self) -> AccountFuture<Result<AccessToken, Error>> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Box::pin(async {
                Err(Error::Network {
                    message: "transient refresh failure".to_string(),
                    transmission_state: bifrost_types::TransmissionState::Unsent,
                    source: None,
                })
            })
        }
    }

    #[tokio::test]
    async fn proactive_refresh_failure_restores_a_still_valid_token() {
        let source = Arc::new(FailingRefreshSource {
            calls: AtomicUsize::new(0),
        });
        let refresher = OAuthRefresher::new(Arc::clone(&source) as Arc<dyn TokenSource>);
        let token = AccessToken::new(
            "still-valid",
            Some(Instant::now() + Duration::from_secs(30)),
        );
        *refresher.state.lock().await = RefreshState::Fresh {
            token,
            refreshed_at: Instant::now(),
            refresh_not_before: None,
        };

        let returned = refresher
            .token()
            .await
            .expect("transient proactive refresh failure should use cached token");

        assert_eq!(returned.as_str(), "still-valid");
        assert_eq!(source.calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            refresher
                .token()
                .await
                .expect("retry suppression reuses the fallback token")
                .as_str(),
            "still-valid"
        );
        assert_eq!(
            source.calls.load(Ordering::SeqCst),
            1,
            "the next request must not immediately retry the failed refresh"
        );
        let state = refresher.state.lock().await;
        let RefreshState::Fresh { token, .. } = &*state else {
            panic!("still-valid token was not restored");
        };
        assert_eq!(token.as_str(), "still-valid");
    }

    #[test]
    fn retry_after_deadline_parses_status_header() {
        let mut headers = HeaderMap::new();
        headers.insert(RETRY_AFTER, HeaderValue::from_static("30"));
        let before = SystemTime::now();
        let deadline = retry_after_deadline(&Error::Status {
            code: reqwest::StatusCode::TOO_MANY_REQUESTS,
            body: Bytes::new(),
            headers,
        })
        .expect("retry-after deadline should parse");
        let after = SystemTime::now();

        let lower = before
            .checked_add(Duration::from_secs(30))
            .expect("test lower bound in range");
        let upper = after
            .checked_add(Duration::from_secs(30))
            .expect("test upper bound in range");
        assert!(deadline >= lower);
        assert!(deadline <= upper);
    }

    #[test]
    fn token_endpoint_unauthorized_preserves_response_evidence() {
        let mut headers = HeaderMap::new();
        headers.insert(
            WWW_AUTHENTICATE,
            HeaderValue::from_static(r#"Bearer error="invalid_grant""#),
        );
        let projected = arc_err_to_error(Arc::new(Error::Status {
            code: reqwest::StatusCode::UNAUTHORIZED,
            body: Bytes::from_static(br#"{"error":"invalid_grant"}"#),
            headers,
        }));

        let Error::AuthLost {
            transmission_state,
            final_response: Some(final_response),
        } = projected
        else {
            panic!("expected auth-lost with final response");
        };
        assert_eq!(transmission_state, None);
        assert_eq!(final_response.status, reqwest::StatusCode::UNAUTHORIZED);
        assert!(final_response.headers.contains_key(WWW_AUTHENTICATE));
        assert_eq!(
            final_response.body,
            Bytes::from_static(br#"{"error":"invalid_grant"}"#)
        );
    }
}
