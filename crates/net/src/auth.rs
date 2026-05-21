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
    /// Stub: the v1 skeleton does not yet drive the state machine.
    /// Phase 2 fills this in.
    pub async fn token(&self) -> Result<AccessToken, Error> {
        unimplemented!("OAuthRefresher::token is filled in by Phase 2")
    }

    /// Force a fresh token, bypassing the proactive-refresh window.
    /// Called by the 401-retry path.
    ///
    /// Stub for the v1 skeleton.
    pub async fn force_refresh(&self) -> Result<AccessToken, Error> {
        unimplemented!("OAuthRefresher::force_refresh is filled in by Phase 2")
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
