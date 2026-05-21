//! Per-account bandwidth metering.
//!
//! HTTP request and response bodies route through the meter so the
//! sync engine can read `bytes/second` per account without each
//! protocol crate maintaining its own counters. IMAP and SMTP feed
//! the same meter through `MeterSink` because the engine needs one
//! reading per account regardless of underlying transport.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicU64;
use std::time::Instant;

use crate::AccountId;

/// Sliding 10-second window of per-second byte counts. Smooths the
/// `observed_bps` reading so a one-shot burst does not pin the
/// reading at the burst peak forever. Fields are written at
/// construction but unused until Phase 2 drives the sample loop.
#[allow(dead_code)]
pub(crate) struct RateWindow {
    /// Ten one-second buckets, indexed by `head`.
    pub(crate) samples: [u64; 10],
    /// Index of the current bucket.
    pub(crate) head: usize,
    /// Instant at which the last sample bucket was rolled.
    pub(crate) last_tick: Instant,
}

impl RateWindow {
    /// Construct an empty window anchored at the given instant.
    pub(crate) fn new(now: Instant) -> Self {
        Self {
            samples: [0; 10],
            head: 0,
            last_tick: now,
        }
    }
}

/// Per-account counter pair. Updated by the metered body-readers in
/// `Net::download_stream`, `RequestBuilder::send`, and the IMAP/SMTP
/// `MeterSink` paths. Allocated by the skeleton; read paths land in
/// Phase 2.
#[allow(dead_code)]
pub(crate) struct AccountCounters {
    pub(crate) bytes_in: AtomicU64,
    pub(crate) bytes_out: AtomicU64,
    pub(crate) window: Mutex<RateWindow>,
}

impl AccountCounters {
    pub(crate) fn new(now: Instant) -> Self {
        Self {
            bytes_in: AtomicU64::new(0),
            bytes_out: AtomicU64::new(0),
            window: Mutex::new(RateWindow::new(now)),
        }
    }
}

/// Top-level meter shared by every `AccountNet` and by IMAP/SMTP
/// `MeterSink` implementations. Holds one `AccountCounters` per
/// registered account.
pub struct BandwidthMeter {
    /// Per-account state. The map grows on `register_account` and
    /// shrinks on `forget_account`; lookups on the hot path go
    /// through `Arc` clones so the map lock is not held while the
    /// counter is updated.
    accounts: Mutex<HashMap<AccountId, Arc<AccountCounters>>>,
}

impl BandwidthMeter {
    /// Construct an empty meter.
    #[must_use]
    pub fn new() -> Self {
        Self {
            accounts: Mutex::new(HashMap::new()),
        }
    }

    /// Register an account with the meter. Idempotent.
    pub fn register_account(&self, account: AccountId) {
        let mut map = self.accounts.lock().expect("meter lock poisoned");
        map.entry(account)
            .or_insert_with(|| Arc::new(AccountCounters::new(Instant::now())));
    }

    /// Drop the per-account counters. Called from
    /// `Net::detach_account`.
    pub fn forget_account(&self, account: AccountId) {
        let mut map = self.accounts.lock().expect("meter lock poisoned");
        map.remove(&account);
    }

    /// Build a handle scoped to one account. Cloneable, cheap.
    #[must_use]
    pub fn account(&self, account: AccountId) -> AccountMeter {
        AccountMeter {
            account,
            counters: self.lookup(account),
        }
    }

    /// Sum bytes/second across every registered account. Used for
    /// process-wide diagnostics; per-account readings go through
    /// `AccountMeter::observed_bps`.
    #[must_use]
    pub fn observed_bps(&self) -> u64 {
        unimplemented!("BandwidthMeter::observed_bps is filled in by Phase 2")
    }

    fn lookup(&self, account: AccountId) -> Option<Arc<AccountCounters>> {
        let map = self.accounts.lock().expect("meter lock poisoned");
        map.get(&account).map(Arc::clone)
    }
}

impl Default for BandwidthMeter {
    fn default() -> Self {
        Self::new()
    }
}

/// Account-scoped view into the meter. Returned from `Net::meter` /
/// `AccountNet::meter` and from `BandwidthMeter::account`.
#[derive(Clone)]
pub struct AccountMeter {
    /// Account this handle is scoped to. Retained for diagnostics and
    /// for `MeterSink` callers that report into a meter shared with
    /// other transports.
    account: AccountId,
    /// Optional handle to per-account counters. `None` if the account
    /// is not registered with the meter; readings then return 0. Held
    /// for the v1 skeleton; Phase 2 drives the read paths through it.
    #[allow(dead_code)]
    counters: Option<Arc<AccountCounters>>,
}

impl AccountMeter {
    /// Sliding-window bytes-per-second reading. Returns 0 if no
    /// samples have landed in the window yet.
    #[must_use]
    pub fn observed_bps(&self) -> u64 {
        unimplemented!("AccountMeter::observed_bps is filled in by Phase 2")
    }

    /// Cumulative bytes received for this account across the meter's
    /// lifetime.
    #[must_use]
    pub fn bytes_in(&self) -> u64 {
        unimplemented!("AccountMeter::bytes_in is filled in by Phase 2")
    }

    /// Cumulative bytes sent for this account across the meter's
    /// lifetime.
    #[must_use]
    pub fn bytes_out(&self) -> u64 {
        unimplemented!("AccountMeter::bytes_out is filled in by Phase 2")
    }

    /// The account this handle is scoped to.
    #[must_use]
    pub fn account(&self) -> AccountId {
        self.account
    }
}

/// Hook used by non-HTTP transports (IMAP, SMTP) to feed
/// bytes-in / bytes-out into the bandwidth meter. The meter lives in
/// `bifrost-net` because the engine already depends on net for the
/// HTTP path; IMAP/SMTP poke a handle rather than carrying a separate
/// metering stack.
pub trait MeterSink: Send + Sync + 'static {
    /// Record `n` inbound bytes for `account`.
    fn record_bytes_in(&self, account: AccountId, n: u64);
    /// Record `n` outbound bytes for `account`.
    fn record_bytes_out(&self, account: AccountId, n: u64);
}
