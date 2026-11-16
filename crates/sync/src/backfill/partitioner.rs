//! Backfill window planners and durable partition keys.
//!
//! Three pure planners turn an account's advertised
//! `InventoryPartitioning` facts into the `InventoryPartition` list the
//! runner requests, with no intermediate representation:
//! - `time_windows`: newest-first, exponentially-widening time windows
//!   ending in an open-ended tail.
//! - `uid_windows`: newest-first half-open UID windows of one UIDVALIDITY
//!   epoch, walking from the advertised maximum UID down to 1.
//! - `page_windows`: forward page-count windows over a positionally
//!   stable result set.
//!
//! `plan.rs` chooses among them from `Account::inventory_partitioning`;
//! there is deliberately no engine-side policy object, because the account
//! is the only party that knows which dimension its inventory can be cut
//! along.

use std::time::Duration;

use bifrost_types::{InventoryPartition, PageWindow, Partition, TimeWindow, UidWindow};
use jiff::{SignedDuration, Timestamp};

/// Default time-window boundaries, measured back from the planning
/// instant: `[7d, 30d, 90d, 180d, 365d, 2y, 3y, 4y, 5y]`. Anything older
/// than the last boundary falls into a final open partition.
#[must_use]
pub fn default_time_boundaries() -> Vec<Duration> {
    const DAY: u64 = 24 * 60 * 60;
    vec![
        Duration::from_secs(7 * DAY),
        Duration::from_secs(30 * DAY),
        Duration::from_secs(90 * DAY),
        Duration::from_secs(180 * DAY),
        Duration::from_secs(365 * DAY),
        Duration::from_secs(2 * 365 * DAY),
        Duration::from_secs(3 * 365 * DAY),
        Duration::from_secs(4 * 365 * DAY),
        Duration::from_secs(5 * 365 * DAY),
    ]
}

/// Plan newest-first time windows ending at `now`, one per boundary plus a
/// final window open at its lower end. Windows chain back to back: each
/// window's `from` is the next window's `to`.
///
/// Server clock skew is expressed by shifting `now` before calling; there
/// is no separate skew parameter because skew only ever moved the anchor.
/// Window edges are whole seconds, the resolution `TimeWindow` carries.
/// A boundary that runs off the start of the representable range leaves
/// that window open at its lower end.
///
/// # Panics
/// If `boundaries` is not ascending, a window would be reversed.
#[must_use]
pub fn time_windows(boundaries: &[Duration], now: Timestamp) -> Vec<InventoryPartition> {
    let mut partitions = Vec::with_capacity(boundaries.len() + 1);
    let mut prev_to = now;
    for boundary in boundaries {
        let from = SignedDuration::try_from(*boundary)
            .ok()
            .and_then(|d| now.checked_sub(d).ok())
            .unwrap_or(Timestamp::MIN);
        partitions.push(time_window(from, prev_to));
        prev_to = from;
    }
    partitions.push(time_window(Timestamp::MIN, prev_to));
    partitions
}

/// Only the LOWER edge is open at the minimum instant; an upper edge that
/// reached it stays a concrete bound, as it always has.
fn time_window(from: Timestamp, to: Timestamp) -> InventoryPartition {
    let from = (from != Timestamp::MIN).then(|| from.as_second());
    InventoryPartition::Time(
        TimeWindow::new(from, Some(to.as_second())).expect("time boundaries must be ascending"),
    )
}

/// Plan newest-first half-open UID windows `[from, to)` of `chunk` UIDs
/// covering `1..=max_uid` in epoch `uid_validity`. Ends are `u64` so the
/// exclusive end of `u32::MAX` is representable. Empty when `max_uid` or
/// `chunk` is zero; the scope walk turns an empty fixed plan into one
/// `Full` request rather than trusting a zero hint.
#[must_use]
pub fn uid_windows(chunk: u32, max_uid: u32, uid_validity: u32) -> Vec<InventoryPartition> {
    if max_uid == 0 || chunk == 0 {
        return Vec::new();
    }
    let mut partitions = Vec::new();
    let mut to = u64::from(max_uid) + 1;
    let chunk = u64::from(chunk);
    while to > 1 {
        let from = to.saturating_sub(chunk).max(1);
        partitions.push(InventoryPartition::Uid(
            UidWindow::new(uid_validity, from, to).expect("ordered window"),
        ));
        if from == 1 {
            break;
        }
        to = from;
    }
    partitions
}

/// Plan forward page windows `[from, to)` of `chunk` items over a result
/// set of `total` items. Empty when either is zero.
#[must_use]
pub fn page_windows(chunk: u32, total: u32) -> Vec<InventoryPartition> {
    if total == 0 || chunk == 0 {
        return Vec::new();
    }
    let mut partitions = Vec::new();
    let mut from: u32 = 0;
    while from < total {
        let to = from.saturating_add(chunk).min(total);
        partitions.push(InventoryPartition::Page(
            PageWindow::new(from, to).expect("ordered window"),
        ));
        from = to;
    }
    partitions
}

pub use super::marker::BackfillKey;

/// Stable durable checkpoint key for an inventory partition.
#[must_use]
pub fn partition_key(partition: &InventoryPartition) -> Partition {
    partition.durable_key()
}

/// Parse a durable `Page` partition key back into its `[from, to)`
/// bounds. The inverse of `partition_key` for
/// `InventoryPartition::Page`; returns `None` for any other partition
/// kind or a malformed key. The backfill orchestrator uses it to resume
/// an open-ended page walk from the furthest durably-checkpointed window
/// rather than re-walking from page 0 on every re-attach.
#[must_use]
pub fn parse_page_partition(partition: &Partition) -> Option<(u32, u32)> {
    match BackfillKey::decode(partition) {
        BackfillKey::Page { from, to } => Some((from, to)),
        _ => None,
    }
}

/// Sentinel partition key marking a scope's backfill as fully exhausted.
/// It does not name a window (no `from:to`); it is a durable "backfill is
/// done" flag the orchestrator writes via the normal consumer-ack path on
/// completion, for both fixed plans and open-ended page walks. Distinct
/// from every real `page:F:T` / `uid:E:F:T` / `time:F:T` key so it never
/// collides with a partition checkpoint. Stores select it by row kind,
/// independently of the number of entries observed during this walk.
#[must_use]
pub fn completion_partition() -> Partition {
    // The unversioned marker could be earned by an empty plan without any
    // inventory. Its bytes cannot distinguish that from a legitimate walk.
    // A new version distinguishes a genuinely empty inventory from that old
    // marker; resume rechecks legacy zero/one markers once for missing history.
    BackfillKey::Complete.encode()
}

/// True if `partition` names a completion sentinel, including the legacy key.
/// Resume separately checks whether a legacy marker proves any inventory.
#[must_use]
pub fn is_completion_partition(partition: &Partition) -> bool {
    BackfillKey::decode(partition).is_completion()
}

#[cfg(test)]
mod tests {
    use super::*;
    use jiff::civil::date;

    fn now() -> Timestamp {
        date(2026, 5, 21)
            .at(12, 0, 0, 0)
            .in_tz("UTC")
            .unwrap()
            .timestamp()
    }

    fn uid(from: u64, to: u64) -> InventoryPartition {
        InventoryPartition::Uid(UidWindow::new(1, from, to).unwrap())
    }

    fn page(from: u32, to: u32) -> InventoryPartition {
        InventoryPartition::Page(PageWindow::new(from, to).unwrap())
    }

    #[test]
    fn time_windowed_default_partitions_in_order() {
        let plan = time_windows(&default_time_boundaries(), now());
        // 9 boundaries + 1 open-ended tail.
        assert_eq!(plan.len(), 10);
        let InventoryPartition::Time(first) = &plan[0] else {
            panic!("expected time partitions");
        };
        assert_eq!(first.to(), Some(now().as_second()));
        for window in plan.windows(2) {
            let (InventoryPartition::Time(newer), InventoryPartition::Time(older)) =
                (&window[0], &window[1])
            else {
                panic!("expected time partitions");
            };
            assert_eq!(
                newer.from(),
                older.to(),
                "partitions must chain back-to-back"
            );
        }
        let InventoryPartition::Time(tail) = plan.last().unwrap() else {
            panic!("expected time partitions");
        };
        assert_eq!(tail.from(), None, "the oldest window is open below");
    }

    #[test]
    fn a_saturated_boundary_opens_only_the_lower_edge() {
        let plan = time_windows(&[Duration::MAX, Duration::MAX], now());
        assert_eq!(
            plan,
            vec![
                InventoryPartition::Time(TimeWindow::new(None, Some(now().as_second())).unwrap()),
                InventoryPartition::Time(
                    TimeWindow::new(None, Some(Timestamp::MIN.as_second())).unwrap()
                ),
                InventoryPartition::Time(
                    TimeWindow::new(None, Some(Timestamp::MIN.as_second())).unwrap()
                ),
            ]
        );
    }

    #[test]
    fn uid_range_chunks_newest_first() {
        assert_eq!(
            uid_windows(1000, 2500, 1),
            vec![uid(1501, 2501), uid(501, 1501), uid(1, 501)]
        );
    }

    #[test]
    fn uid_windows_reach_the_top_of_the_uid_space() {
        let plan = uid_windows(u32::MAX, u32::MAX, 1);
        assert_eq!(plan, vec![uid(1, u64::from(u32::MAX) + 1)]);
    }

    #[test]
    fn page_count_walks_in_order() {
        assert_eq!(
            page_windows(250, 700),
            vec![page(0, 250), page(250, 500), page(500, 700)]
        );
    }

    #[test]
    fn partition_keys_are_stable() {
        assert_eq!(
            partition_key(&InventoryPartition::Full),
            BackfillKey::Full.encode()
        );
        assert_eq!(
            partition_key(&page(25, 50)),
            Partition(b"page:25:50".to_vec())
        );
        assert_eq!(partition_key(&uid(7, 9)), Partition(b"uid:1:7:9".to_vec()));
        assert_eq!(
            partition_key(&InventoryPartition::Time(
                TimeWindow::new(None, Some(123)).expect("ordered window")
            )),
            Partition(b"time::123".to_vec())
        );
        for partition in [
            InventoryPartition::Full,
            InventoryPartition::Time(TimeWindow::new(None, None).unwrap()),
            InventoryPartition::Time(TimeWindow::new(Some(-456), None).unwrap()),
            InventoryPartition::Time(TimeWindow::new(Some(-456), Some(123)).unwrap()),
            InventoryPartition::Uid(UidWindow::new(99, 1, u64::from(u32::MAX) + 1).unwrap()),
            InventoryPartition::Page(PageWindow::new(0, u32::MAX).unwrap()),
        ] {
            let key = partition_key(&partition);
            assert!(!matches!(BackfillKey::decode(&key), BackfillKey::Opaque(_)));
            assert_eq!(BackfillKey::decode(&key).encode(), key);
        }
    }

    #[test]
    fn page_partition_key_round_trips() {
        let key = partition_key(&page(500, 1000));
        assert_eq!(parse_page_partition(&key), Some((500, 1000)));
    }

    #[test]
    fn parse_page_partition_rejects_other_kinds() {
        assert_eq!(parse_page_partition(&partition_key(&uid(7, 9))), None);
        assert_eq!(
            parse_page_partition(&partition_key(&InventoryPartition::Full)),
            None
        );
        assert_eq!(parse_page_partition(&Partition(b"page:1".to_vec())), None);
        assert_eq!(parse_page_partition(&Partition(b"page:a:b".to_vec())), None);
    }

    #[test]
    fn completion_sentinel_is_distinct_from_pages() {
        let complete = completion_partition();
        assert!(is_completion_partition(&complete));
        assert_eq!(parse_page_partition(&complete), None);
        assert!(!is_completion_partition(&partition_key(&page(0, 500))));
    }

    #[test]
    fn empty_inputs_yield_empty_plan() {
        assert!(uid_windows(100, 0, 1).is_empty());
        assert!(uid_windows(0, 100, 1).is_empty());
        assert!(page_windows(50, 0).is_empty());
        assert!(page_windows(0, 50).is_empty());
    }
}
