//! OAuth bearer-token plumbing with single-flight refresh.
//!
//! The token source is a trait so different OAuth flows (refresh
//! token, device code, service-account JWT) can share one bearer
//! pipeline. `OAuthRefresher` coordinates concurrent refresh
//! attempts behind one `tokio::sync::Mutex` so N in-flight requests
//! do not trigger N independent refreshes.

use std::fmt;
use std::sync::Arc;
use std::time::Instant;

use bifrost_types::AccountFuture;
use tokio::sync::{Mutex, oneshot};
use zeroize::Zeroizing;

use crate::error::Error;

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
/// state transitions to `Refreshing { waiters }` and exactly one task
/// drives the network call. Concurrent callers register a oneshot
/// receiver in `waiters` and await.
pub struct OAuthRefresher {
    /// Underlying provider hooked up at construction.
    source: Arc<dyn TokenSource>,
    /// Shared state behind the single-flight lock.
    state: Arc<Mutex<RefreshState>>,
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
        }
    }

    /// Return the current access token, refreshing if the cached copy
    /// is within the proactive-refresh window or absent.
    ///
    /// Single-flights concurrent refresh attempts. The first caller
    /// to find the state empty or stale transitions the state to
    /// `Refreshing` and drives the network round-trip itself.
    /// Concurrent callers register a oneshot receiver and await.
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
                } if !force && !needs_refresh(token, *refreshed_at) => {
                    // Steady state: cached token is fresh enough.
                    return Ok(token.clone());
                }
                RefreshState::Fresh { .. } | RefreshState::Empty => {
                    // We are the driving task. Mark the state as
                    // `Refreshing` with an empty waiter list and drop
                    // the lock.
                    *state = RefreshState::Refreshing {
                        waiters: Vec::new(),
                    };
                    DriverRole::Driver
                }
                RefreshState::Refreshing { waiters } => {
                    let (tx, rx) = oneshot::channel();
                    waiters.push(tx);
                    DriverRole::Waiter(rx)
                }
            }
        };

        match role {
            DriverRole::Driver => self.drive_refresh().await,
            DriverRole::Waiter(rx) => match rx.await {
                Ok(Ok(t)) => Ok(t),
                Ok(Err(e)) => Err(arc_err_to_error(e)),
                // The driver task dropped without sending. Treat as
                // a refresh failure; the caller will see AuthLost.
                Err(_) => Err(Error::AuthLost),
            },
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

    /// Internal state handle. Crate-public so `Net` and `RequestBuilder`
    /// can drive the state machine when Phase 2 wires the refresh
    /// path.
    pub(crate) fn state(&self) -> &Mutex<RefreshState> {
        &self.state
    }
}

// `OAuthRefresher` itself implements `TokenSource` so call sites can
// freely substitute a refresher for a raw token source. `current()`
// returns the cached token (refreshing only if stale); `refresh()`
// forces a network round-trip.
impl TokenSource for OAuthRefresher {
    fn current(&self) -> AccountFuture<Result<AccessToken, Error>> {
        // Clone the source-bearing fields so the returned future is
        // `'static`. The trait return is `Pin<Box<...>>` for dyn
        // safety; we don't capture `&self`.
        let me = OAuthRefresher {
            source: Arc::clone(&self.source),
            state: Arc::clone(&self.state),
        };
        Box::pin(async move { me.token().await })
    }

    fn refresh(&self) -> AccountFuture<Result<AccessToken, Error>> {
        let me = OAuthRefresher {
            source: Arc::clone(&self.source),
            state: Arc::clone(&self.state),
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
    /// drive the network round-trip.
    Driver,
    /// Another task is already driving the refresh; this caller is
    /// parked on a oneshot until the driver finishes.
    Waiter(oneshot::Receiver<Result<AccessToken, Arc<Error>>>),
}

/// Has the cached token aged past the proactive-refresh threshold?
///
/// The threshold is 60 seconds before the issuer-supplied expiry. If
/// the issuer did not supply an expiry, the cached token is treated as
/// fresh indefinitely: opaque tokens with no TTL refresh only on a
/// 401 response, which is `force_refresh`'s job.
fn needs_refresh(token: &AccessToken, _refreshed_at: Instant) -> bool {
    let Some(expires_at) = token.expires_at() else {
        return false;
    };
    let now = Instant::now();
    let window = std::time::Duration::from_secs(60);
    // Refresh when `now + window >= expires_at`. Saturating math
    // avoids panics on near-overflow Instant arithmetic.
    let deadline = expires_at.checked_sub(window).unwrap_or(expires_at);
    now >= deadline
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
