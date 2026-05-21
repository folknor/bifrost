//! OAuth bearer-token plumbing with single-flight refresh.
//!
//! The token source is a trait so different OAuth flows (refresh
//! token, device code, service-account JWT) can share one bearer
//! pipeline. `OAuthRefresher` coordinates concurrent refresh
//! attempts behind one `tokio::sync::Mutex` so N in-flight requests
//! do not trigger N independent refreshes.

use std::fmt;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bifrost_types::AccountFuture;
use tokio::sync::{Mutex, oneshot};
use zeroize::Zeroizing;

use crate::error::Error;

/// Default age at which a token without an `expires_at` is treated as
/// stale enough to refresh. Typical OAuth access tokens have a 60-min
/// TTL; refreshing at 55 min leaves a 5-min safety margin without
/// burning the issuer's quota on every request. Wired through
/// `OAuthRefresher::with_max_age` so callers can lengthen or shorten
/// it.
pub const DEFAULT_TOKEN_MAX_AGE: Duration = Duration::from_secs(55 * 60);

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
                } if !force && !needs_refresh(token, *refreshed_at, self.max_age) => {
                    // Steady state: cached token is fresh enough.
                    return Ok(token.clone());
                }
                RefreshState::Fresh { .. } | RefreshState::Empty => {
                    // This caller is responsible for spawning the
                    // refresh driver. Register it as the first waiter
                    // before dropping the lock so cancellation of this
                    // future cannot leave the state stuck in
                    // `Refreshing`.
                    let (tx, rx) = oneshot::channel();
                    *state = RefreshState::Refreshing { waiters: vec![tx] };
                    DriverRole::Driver(rx)
                }
                RefreshState::Refreshing { waiters } => {
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
        let waiters = match std::mem::replace(&mut *state, RefreshState::Empty) {
            RefreshState::Refreshing { waiters } => waiters,
            // The only way this can happen is if a second driver
            // claimed the role concurrently, which the single-flight
            // protocol forbids. Defensive.
            _ => Vec::new(),
        };

        match result {
            Ok(token) => {
                *state = RefreshState::Fresh {
                    token: token.clone(),
                    refreshed_at: Instant::now(),
                };
                drop(state);
                for waiter in waiters {
                    let _ = waiter.send(Ok(token.clone()));
                }
                Ok(token)
            }
            Err(err) => {
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
        Err(_) => Err(Error::AuthLost),
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
/// engine needs. We preserve them inside `RefreshFailed { source }`
/// so the caller can either pattern-match on the inner variant for a
/// retry decision or treat the wrapper as a single "refresh failed"
/// class.
fn arc_err_to_error(err: Arc<Error>) -> Error {
    match &*err {
        Error::AuthLost => Error::AuthLost,
        Error::Status { code, .. }
            if *code == reqwest::StatusCode::UNAUTHORIZED
                || *code == reqwest::StatusCode::FORBIDDEN =>
        {
            Error::AuthLost
        }
        _ => Error::RefreshFailed { source: err },
    }
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
    },
}
