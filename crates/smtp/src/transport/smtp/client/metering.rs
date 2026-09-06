//! Per-account byte accounting and bandwidth capping for the raw SMTP
//! socket.
//!
//! SMTP was the only protocol in the workspace whose bytes reached the
//! wire unmetered, which made `Account::set_bandwidth_cap` silently
//! PARTIAL rather than absent: an IMAP-shaped account honoured the cap on
//! its fetch traffic and ignored it on the send path, and the send path
//! is the upstream-heavy one a cap usually exists to protect.
//!
//! The design difference from IMAP's `WireMetering`, which this otherwise
//! mirrors: the bucket here RETURNS the debt it wants slept rather than
//! sleeping itself. SMTP has three call shapes over one bucket - an
//! `async fn`, an `AsyncWrite::poll_write` that cannot await, and a
//! blocking `Write` - and a debt-returning bucket serves all three
//! without a second implementation to drift from this one.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use bifrost_net::MeterSinkHandle;

/// Sentinel meaning "no cap" in the shared atomic. Matches the encoding
/// `bifrost-imap` uses so a consumer can drive both from one value.
// pub: consumers write this into the shared cap atomic to mean "no cap".
pub const UNLIMITED_BANDWIDTH: u64 = u64::MAX;

fn bandwidth_cap_from_raw(raw: u64) -> Option<u64> {
    (raw != UNLIMITED_BANDWIDTH).then_some(raw.max(1))
}

/// Byte accounting plus optional throttling for one connection.
///
/// Cheap to clone; every clone shares the same buckets and cap, so a
/// pooled transport's connections throttle against ONE budget rather
/// than each getting the full cap.
#[derive(Clone)]
pub(crate) struct WireMetering {
    sink: Option<MeterSinkHandle>,
    bandwidth_cap: Option<Arc<AtomicU64>>,
    in_bucket: ByteBucket,
    out_bucket: ByteBucket,
}

impl WireMetering {
    /// No sink and no cap: every operation is a no-op returning no debt.
    /// The shape every non-account caller of this crate gets.
    pub(crate) fn disabled() -> Self {
        Self::new(None, None)
    }

    pub(crate) fn new(
        sink: Option<MeterSinkHandle>,
        bandwidth_cap: Option<Arc<AtomicU64>>,
    ) -> Self {
        let initial = bandwidth_cap
            .as_ref()
            .and_then(|cap| bandwidth_cap_from_raw(cap.load(Ordering::Relaxed)));
        Self {
            sink,
            bandwidth_cap,
            in_bucket: ByteBucket::new(initial),
            out_bucket: ByteBucket::new(initial),
        }
    }

    /// True when anything is being recorded or capped. Lets the hot path
    /// skip the bucket entirely on the common unmetered build.
    pub(crate) fn is_enabled(&self) -> bool {
        self.sink.is_some() || self.bandwidth_cap.is_some()
    }

    /// Account for `n` inbound bytes; returns how long the caller must
    /// wait before reading more, if the cap says so.
    pub(crate) fn record_in(&self, n: usize) -> Option<Duration> {
        let n = u64::try_from(n).unwrap_or(u64::MAX);
        if let Some(sink) = &self.sink {
            sink.record_bytes_in(n);
        }
        self.in_bucket.take(n, self.cap_now())
    }

    /// Account for `n` outbound bytes; returns the wait the cap imposes.
    pub(crate) fn record_out(&self, n: usize) -> Option<Duration> {
        let n = u64::try_from(n).unwrap_or(u64::MAX);
        if let Some(sink) = &self.sink {
            sink.record_bytes_out(n);
        }
        self.out_bucket.take(n, self.cap_now())
    }

    /// How many bytes a single socket write may offer, given the cap in
    /// force right now: one second of budget, or `None` when uncapped.
    ///
    /// The bucket lets tokens go negative, so a write charged for several
    /// megabytes parks a sleep of several *seconds* - and the async funnel
    /// makes the next write wait that sleep out before it touches the
    /// socket, inside the caller's per-operation write timeout. A healthy
    /// throttled upload then looks exactly like a stalled peer. Clamping
    /// what is offered bounds any parked debt at about a second, so the
    /// write timeout is only ever spent on the peer. It does not clamp the
    /// debt itself: a write at or below this limit still owes proportional
    /// time, which is what keeps a 1 B/s cap at 1 B/s.
    ///
    /// Re-read per call for the same reason `cap_now` is.
    pub(crate) fn write_chunk_limit(&self) -> Option<usize> {
        self.cap_now()
            .map(|cap| usize::try_from(cap).unwrap_or(usize::MAX))
    }

    /// Read the cap fresh on every call: a consumer can retune it at any
    /// time through `Account::set_bandwidth_cap`, and a connection that
    /// snapshotted it at construction would hold a stale budget for its
    /// whole pooled lifetime.
    fn cap_now(&self) -> Option<u64> {
        self.bandwidth_cap
            .as_ref()
            .and_then(|cap| bandwidth_cap_from_raw(cap.load(Ordering::Relaxed)))
    }
}

impl std::fmt::Debug for WireMetering {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WireMetering")
            .field("metered", &self.sink.is_some())
            .field("capped", &self.bandwidth_cap.is_some())
            .finish()
    }
}

#[derive(Clone)]
struct ByteBucket {
    state: Arc<Mutex<ByteBucketState>>,
}

struct ByteBucketState {
    /// Allowed to go NEGATIVE. A transfer larger than one second of
    /// budget borrows against future seconds and the debt is slept in
    /// full, rather than being clamped to a single second's wait - a
    /// clamp would silently let a low cap be exceeded (a 1 B/s cap with
    /// 16 KiB writes would behave like several hundred B/s).
    tokens: f64,
    last_refill: Instant,
    /// The cap `tokens` is denominated in. Tokens are BYTES and debt is
    /// read back as `-tokens / cap`, so a cap change between two charges
    /// would re-price debt already owed: 1 MB charged at 1 MB/s is one
    /// second of debt, and re-reading it at a 1 KB/s cap makes it ~1000
    /// seconds - which the next write then waits out inside the caller's
    /// per-operation timeout, the exact failure `write_chunk_limit`
    /// exists to prevent. Rescaling on the way in keeps the debt fixed in
    /// SECONDS, so a retune changes the future rate without repricing the
    /// past.
    priced_at: Option<f64>,
}

impl ByteBucket {
    fn new(cap: Option<u64>) -> Self {
        Self {
            state: Arc::new(Mutex::new(ByteBucketState {
                tokens: cap.map_or(0.0, |cap| cap as f64),
                last_refill: Instant::now(),
                priced_at: cap.map(|cap| cap as f64),
            })),
        }
    }

    /// Refill by elapsed time, debit `n`, and report the resulting debt.
    ///
    /// Synchronous by design: the caller does the waiting, so the mutex
    /// guard cannot be held across a sleep or an await.
    fn take(&self, n: u64, cap: Option<u64>) -> Option<Duration> {
        let cap = cap?;
        if cap == 0 || n == 0 {
            return None;
        }
        let cap_f = cap as f64;
        let mut state = self.lock_state();
        // Re-denominate any standing balance into the cap now in force
        // before it is read as a duration. See `priced_at`.
        if let Some(previous) = state.priced_at
            && previous > 0.0
            && (previous - cap_f).abs() > f64::EPSILON
        {
            state.tokens *= cap_f / previous;
        }
        state.priced_at = Some(cap_f);
        let now = Instant::now();
        let elapsed = now.duration_since(state.last_refill).as_secs_f64();
        // Credit never exceeds one second of budget, so an idle
        // connection cannot bank capacity and then burst past the cap.
        state.tokens = (state.tokens + elapsed * cap_f).min(cap_f);
        state.last_refill = now;
        state.tokens -= n as f64;
        (state.tokens < 0.0).then(|| Duration::from_secs_f64(-state.tokens / cap_f))
    }

    fn lock_state(&self) -> MutexGuard<'_, ByteBucketState> {
        self.state.lock().unwrap_or_else(|poisoned| {
            #[cfg(feature = "tracing")]
            tracing::warn!("SMTP byte bucket lock poisoned; recovering metering state");
            poisoned.into_inner()
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_disabled_metering_never_throttles() {
        let metering = WireMetering::disabled();
        assert!(!metering.is_enabled());
        assert_eq!(metering.record_out(1_000_000), None);
        assert_eq!(metering.record_in(1_000_000), None);
    }

    /// A cap of `UNLIMITED_BANDWIDTH` is the "no cap" sentinel, not a cap
    /// of `u64::MAX` bytes per second.
    #[test]
    fn the_unlimited_sentinel_is_not_a_very_large_cap() {
        let cap = Arc::new(AtomicU64::new(UNLIMITED_BANDWIDTH));
        let metering = WireMetering::new(None, Some(cap));
        assert_eq!(metering.record_out(4096), None);
    }

    /// Spending within one second of budget is free; the overspend is
    /// what owes time.
    #[test]
    fn a_write_inside_the_budget_does_not_wait() {
        let cap = Arc::new(AtomicU64::new(1000));
        let metering = WireMetering::new(None, Some(cap));
        assert_eq!(metering.record_out(500), None);
    }

    /// The property the debt model exists for: a transfer far larger than
    /// the cap owes proportional time, NOT one second. Clamping here is
    /// what would let a 1 B/s cap run at hundreds of B/s.
    #[test]
    fn an_oversized_write_owes_time_proportional_to_its_size() {
        let cap = Arc::new(AtomicU64::new(1000));
        let metering = WireMetering::new(None, Some(cap));

        // Burn the initial second of budget, then overspend by 10x.
        assert_eq!(metering.record_out(1000), None);
        let debt = metering.record_out(10_000).expect("an overspend owes time");
        assert!(
            debt >= Duration::from_secs(9) && debt <= Duration::from_secs(11),
            "10_000 bytes at 1000 B/s is about 10s, got {debt:?}"
        );
    }

    /// In and out are separate budgets - a large send must not throttle
    /// the reply that follows it.
    #[test]
    fn the_inbound_and_outbound_budgets_are_independent() {
        let cap = Arc::new(AtomicU64::new(1000));
        let metering = WireMetering::new(None, Some(cap));

        assert_eq!(metering.record_out(1000), None);
        assert!(
            metering.record_out(5000).is_some(),
            "outbound is now in debt"
        );
        assert_eq!(
            metering.record_in(500),
            None,
            "inbound has its own untouched budget"
        );
    }

    /// Debt is owed in SECONDS, not in bytes-at-whatever-cap-is-current.
    /// Tokens are bytes and the debt is `-tokens / cap`, so lowering the
    /// cap between two charges used to re-price debt already owed: one
    /// second's worth at the old cap read back as a thousand seconds at
    /// the new one, which the next write then waits out inside the
    /// caller's per-operation write timeout - the very failure the
    /// one-second offer clamp exists to prevent.
    #[test]
    fn lowering_the_cap_does_not_reprice_debt_already_owed() {
        let cap = Arc::new(AtomicU64::new(1_000_000));
        let metering = WireMetering::new(None, Some(Arc::clone(&cap)));

        // Burn the initial second, then owe about one more second at 1 MB/s.
        assert_eq!(metering.record_out(1_000_000), None);
        let debt = metering
            .record_out(1_000_000)
            .expect("a second megabyte overspends");
        assert!(
            debt <= Duration::from_millis(1100),
            "1 MB at 1 MB/s is about a second, got {debt:?}"
        );

        // A consumer retunes down by three orders of magnitude. The debt
        // standing from the previous charge is a second of time, and it
        // must stay a second of time.
        cap.store(1_000, Ordering::Relaxed);
        let debt = metering.record_out(1).expect("still in debt");
        assert!(
            debt <= Duration::from_millis(1100),
            "the outstanding debt was priced at the old cap; got {debt:?}"
        );
    }

    /// The cap is read per call, so raising it mid-connection takes
    /// effect without reconnecting.
    #[test]
    fn a_retuned_cap_takes_effect_on_the_next_call() {
        let cap = Arc::new(AtomicU64::new(1000));
        let metering = WireMetering::new(None, Some(Arc::clone(&cap)));
        assert_eq!(metering.record_out(1000), None);
        assert!(metering.record_out(4000).is_some());

        cap.store(UNLIMITED_BANDWIDTH, Ordering::Relaxed);
        assert_eq!(
            metering.record_out(1_000_000),
            None,
            "lifting the cap stops throttling immediately"
        );
    }
}
