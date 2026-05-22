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
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use crate::AccountId;

/// One second's worth of byte counts. Ten of these in a ring buffer
/// form the sliding `observed_bps` window. Tracking in/out separately
/// makes "uploading 50 MB while downloading something else" readable.
#[derive(Copy, Clone, Default)]
pub(crate) struct WindowSample {
    pub(crate) bytes_in: u64,
    pub(crate) bytes_out: u64,
}

/// Sliding 10-second window of per-second byte counts. Smooths the
/// `observed_bps` reading so a one-shot burst does not pin the
/// reading at the burst peak forever.
pub(crate) struct RateWindow {
    /// Ten one-second buckets indexed by `head`.
    pub(crate) samples: [WindowSample; 10],
    /// Index of the bucket currently being filled.
    pub(crate) head: usize,
    /// Instant at which the head bucket was opened. Each second we
    /// advance `head` and zero the newly current bucket.
    pub(crate) head_started: Instant,
    /// Instant the window itself was first constructed. Used so the
    /// warm-up bps reading divides by the elapsed window instead of
    /// the full ten seconds and under-reports.
    pub(crate) created_at: Instant,
}

impl RateWindow {
    /// Construct an empty window anchored at the given instant.
    pub(crate) fn new(now: Instant) -> Self {
        Self {
            samples: [WindowSample::default(); 10],
            head: 0,
            head_started: now,
            created_at: now,
        }
    }

    /// Roll the head bucket forward by the number of whole seconds
    /// that have elapsed since the head was last opened. Zeros each
    /// intermediate bucket. Bounded at 10 steps because beyond that
    /// the entire window is stale anyway.
    fn advance(&mut self, now: Instant) {
        let elapsed = now.duration_since(self.head_started);
        let steps_full = elapsed.as_secs();
        if steps_full == 0 {
            return;
        }
        let steps = usize::try_from(steps_full).unwrap_or(10).min(10);
        for _ in 0..steps {
            self.head = (self.head + 1) % self.samples.len();
            self.samples[self.head] = WindowSample::default();
        }
        // Advance `head_started` by the full elapsed seconds, leaving
        // the sub-second remainder so the next advance accumulates
        // correctly.
        self.head_started += Duration::from_secs(steps_full);
    }

    /// Add inbound bytes to the current head bucket. Rolls first.
    pub(crate) fn add_in(&mut self, now: Instant, bytes: u64) {
        self.advance(now);
        self.samples[self.head].bytes_in = self.samples[self.head].bytes_in.saturating_add(bytes);
    }

    /// Add outbound bytes to the current head bucket. Rolls first.
    pub(crate) fn add_out(&mut self, now: Instant, bytes: u64) {
        self.advance(now);
        self.samples[self.head].bytes_out = self.samples[self.head].bytes_out.saturating_add(bytes);
    }

    /// Bytes-per-second across the trailing 10-second window. Sums
    /// in + out across every bucket and divides by the elapsed
    /// window length, capped at ten seconds. During the warm-up
    /// period (less than ten seconds since construction) the divisor
    /// is the actual elapsed wall-clock time, so a fresh meter does
    /// not under-report a small burst.
    fn bps(&mut self, now: Instant) -> u64 {
        self.advance(now);
        let total: u64 = self
            .samples
            .iter()
            .map(|s| s.bytes_in.saturating_add(s.bytes_out))
            .sum();
        // Divisor in whole seconds. Saturating to >=1 sec keeps the
        // first sub-second sample from producing a huge spike.
        let elapsed = now.duration_since(self.created_at).as_secs().max(1);
        let divisor = elapsed.min(10);
        total / divisor
    }
}

/// Per-account counter pair. Updated by the metered body-readers in
/// `Net::download_stream`, `RequestBuilder::send`, and the IMAP/SMTP
/// `MeterSink` paths.
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

    fn record_in(&self, n: u64) {
        self.bytes_in.fetch_add(n, Ordering::Relaxed);
        let now = Instant::now();
        if let Ok(mut w) = self.window.lock() {
            w.add_in(now, n);
        }
    }

    fn record_out(&self, n: u64) {
        self.bytes_out.fetch_add(n, Ordering::Relaxed);
        let now = Instant::now();
        if let Ok(mut w) = self.window.lock() {
            w.add_out(now, n);
        }
    }

    fn bps(&self) -> u64 {
        let now = Instant::now();
        match self.window.lock() {
            Ok(mut w) => w.bps(now),
            Err(_) => 0,
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
    pub fn forget_account(&self, account: &AccountId) {
        let mut map = self.accounts.lock().expect("meter lock poisoned");
        map.remove(account);
    }

    /// Build a handle scoped to one account. Cloneable, cheap.
    #[must_use]
    pub fn account(&self, account: AccountId) -> AccountMeter {
        let counters = self.lookup(&account);
        AccountMeter { account, counters }
    }

    /// Sum bytes/second across every registered account. Used for
    /// process-wide diagnostics; per-account readings go through
    /// `AccountMeter::observed_bps`.
    #[must_use]
    pub fn observed_bps(&self) -> u64 {
        let map = self.accounts.lock().expect("meter lock poisoned");
        let mut total: u64 = 0;
        for c in map.values() {
            total = total.saturating_add(c.bps());
        }
        total
    }

    fn lookup(&self, account: &AccountId) -> Option<Arc<AccountCounters>> {
        let map = self.accounts.lock().expect("meter lock poisoned");
        map.get(account).map(Arc::clone)
    }

    fn lookup_or_register(&self, account: &AccountId) -> Arc<AccountCounters> {
        let mut map = self.accounts.lock().expect("meter lock poisoned");
        Arc::clone(
            map.entry(account.clone())
                .or_insert_with(|| Arc::new(AccountCounters::new(Instant::now()))),
        )
    }
}

impl Default for BandwidthMeter {
    fn default() -> Self {
        Self::new()
    }
}

impl MeterSink for BandwidthMeter {
    fn record_bytes_in(&self, account: &AccountId, n: u64) {
        self.lookup_or_register(account).record_in(n);
    }
    fn record_bytes_out(&self, account: &AccountId, n: u64) {
        self.lookup_or_register(account).record_out(n);
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
    /// Per-account counters. `None` if the account is not registered
    /// with the meter; readings then return 0.
    counters: Option<Arc<AccountCounters>>,
}

impl AccountMeter {
    /// Sliding-window bytes-per-second reading. Returns 0 if no
    /// samples have landed in the window yet.
    #[must_use]
    pub fn observed_bps(&self) -> u64 {
        self.counters.as_deref().map_or(0, AccountCounters::bps)
    }

    /// Cumulative bytes received for this account across the meter's
    /// lifetime.
    #[must_use]
    pub fn bytes_in(&self) -> u64 {
        self.counters
            .as_ref()
            .map_or(0, |c| c.bytes_in.load(Ordering::Relaxed))
    }

    /// Cumulative bytes sent for this account across the meter's
    /// lifetime.
    #[must_use]
    pub fn bytes_out(&self) -> u64 {
        self.counters
            .as_ref()
            .map_or(0, |c| c.bytes_out.load(Ordering::Relaxed))
    }

    /// The account this handle is scoped to.
    #[must_use]
    pub fn account(&self) -> &AccountId {
        &self.account
    }

    /// Record `n` inbound bytes against this account. Crate-internal:
    /// the metered body-readers in `request.rs` and `net.rs` call
    /// this on every chunk.
    pub(crate) fn record_bytes_in(&self, n: u64) {
        if let Some(c) = self.counters.as_ref() {
            c.record_in(n);
        }
    }

    /// Record `n` outbound bytes against this account.
    pub(crate) fn record_bytes_out(&self, n: u64) {
        if let Some(c) = self.counters.as_ref() {
            c.record_out(n);
        }
    }
}

/// Hook used by non-HTTP transports (IMAP, SMTP) to feed
/// bytes-in / bytes-out into the bandwidth meter. The meter lives in
/// `bifrost-net` because the engine already depends on net for the
/// HTTP path; IMAP/SMTP poke a handle rather than carrying a separate
/// metering stack.
///
/// The trait is dyn-safe so a protocol crate can hold
/// `Arc<dyn MeterSink>` without naming the concrete `BandwidthMeter`
/// type, and so test doubles can substitute one in. The HTTP path
/// goes through `AccountMeter` directly because it already owns an
/// `Arc<AccountCounters>` from `attach_account`; raw-socket
/// transports go through a `MeterSinkHandle` which composes the
/// account id with the sink so the transport call site does not
/// need to thread the id alongside every byte count.
pub trait MeterSink: Send + Sync + 'static {
    /// Record `n` inbound bytes for `account`.
    fn record_bytes_in(&self, account: &AccountId, n: u64);
    /// Record `n` outbound bytes for `account`.
    fn record_bytes_out(&self, account: &AccountId, n: u64);
}

/// Account-scoped adapter around a `MeterSink`.
///
/// Raw-socket transports (IMAP, SMTP) construct one of these per
/// connection and call `record_bytes_in` / `record_bytes_out` on
/// every wire read / write. The handle owns the account id so the
/// transport call site does not have to thread it. Cloneable; one
/// per spawned task is the expected usage.
///
/// `S1-W1 status`: the adapter is shipped for use; IMAP / SMTP
/// wiring lands in S1-W2.
#[derive(Clone)]
pub struct MeterSinkHandle {
    sink: Arc<dyn MeterSink>,
    account: AccountId,
}

impl MeterSinkHandle {
    /// Construct an account-scoped handle around a sink.
    #[must_use]
    pub fn new(sink: Arc<dyn MeterSink>, account: AccountId) -> Self {
        Self { sink, account }
    }

    /// Construct a handle backed by the process-wide
    /// `BandwidthMeter` on `Net`. Convenience for raw-socket
    /// transports that already have a `Net` reference.
    #[must_use]
    pub fn from_meter(meter: Arc<BandwidthMeter>, account: AccountId) -> Self {
        Self {
            sink: meter as Arc<dyn MeterSink>,
            account,
        }
    }

    /// Account this handle is scoped to.
    #[must_use]
    pub fn account(&self) -> &AccountId {
        &self.account
    }

    /// Record `n` inbound bytes against the scoped account.
    pub fn record_bytes_in(&self, n: u64) {
        self.sink.record_bytes_in(&self.account, n);
    }

    /// Record `n` outbound bytes against the scoped account.
    pub fn record_bytes_out(&self, n: u64) {
        self.sink.record_bytes_out(&self.account, n);
    }
}

impl std::fmt::Debug for MeterSinkHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MeterSinkHandle")
            .field("account", &self.account)
            .finish_non_exhaustive()
    }
}
