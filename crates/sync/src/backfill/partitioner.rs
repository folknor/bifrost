//! Backfill partitioner.
//!
//! Plans newest-first partitions for a backfill pass. Three strategies:
//! - `TimeWindowed` (default): exponentially-widening (7d, 30d, 90d,
//!   180d, 365d, then yearly).
//! - `UidRange` (IMAP-on-Basic fallback): walks UID 1:N in fixed-size
//!   chunks.
//! - `PageCount` (JMAP optimization): walks a page count instead of a
//!   time / UID dimension.
//!
//! The planner is pure; the runner consumes a `PartitionPlan`.

use std::time::Duration;

use bifrost_types::{InventoryPartition, Partition};
use chrono::{DateTime, Utc};

/// Account-level backfill policy.
#[derive(Debug, Clone)]
pub struct BackfillPolicy {
    pub strategy: BackfillStrategy,
    /// Server-vs-client clock skew applied to time-windowed bounds.
    /// Positive means the server is ahead of the client.
    pub clock_skew: Duration,
}

impl Default for BackfillPolicy {
    fn default() -> Self {
        Self {
            strategy: BackfillStrategy::TimeWindowed {
                boundaries: default_time_boundaries(),
            },
            clock_skew: Duration::ZERO,
        }
    }
}

/// Partitioning strategy chosen per account.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum BackfillStrategy {
    /// Exponentially-widening time partitions.
    TimeWindowed { boundaries: Vec<Duration> },
    /// IMAP-on-Basic fallback. Walks UID 1:N in chunks.
    UidRange { chunk_size: u32 },
    /// JMAP `queryChanges` page count. Walks a count over the result
    /// set rather than a time/UID dimension.
    PageCount { items_per_partition: u32 },
}

/// Default time-windowed boundaries from `bifrost-sync.md`:
/// `[7d, 30d, 90d, 180d, 365d, 2y, 3y, 4y, 5y]`. Anything older than
/// the last boundary falls into a final open partition.
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

/// A concrete partition plan. The runner walks `partitions` newest-first.
#[derive(Debug, Clone)]
pub struct PartitionPlan {
    pub partitions: Vec<PartitionBounds>,
}

/// One partition's bounds. Three variants matching `BackfillStrategy`.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum PartitionBounds {
    /// `[from, to)` inclusive-exclusive time window.
    Time {
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    },
    /// `[from, to]` inclusive UID range.
    Uid { from: u32, to: u32 },
    /// `[from, to)` page-count range.
    Page { from: u32, to: u32 },
}

/// Convert planner bounds into the protocol-neutral Account trait
/// partition shape.
#[must_use]
pub fn inventory_partition_for(bounds: &PartitionBounds) -> InventoryPartition {
    match bounds {
        PartitionBounds::Time { from, to } => InventoryPartition::Time {
            from_unix_seconds: if *from == DateTime::<Utc>::MIN_UTC {
                None
            } else {
                Some(from.timestamp())
            },
            to_unix_seconds: Some(to.timestamp()),
        },
        PartitionBounds::Uid { from, to } => InventoryPartition::Uid {
            from: *from,
            to: *to,
        },
        PartitionBounds::Page { from, to } => InventoryPartition::Page {
            from: *from,
            to: *to,
        },
    }
}

/// Stable durable checkpoint key for an inventory partition.
#[must_use]
pub fn partition_key(partition: &InventoryPartition) -> Partition {
    let key = match partition {
        InventoryPartition::Full => "full".to_string(),
        InventoryPartition::Time {
            from_unix_seconds,
            to_unix_seconds,
        } => format!(
            "time:{}:{}",
            optional_i64(*from_unix_seconds),
            optional_i64(*to_unix_seconds)
        ),
        InventoryPartition::Uid { from, to } => format!("uid:{from}:{to}"),
        InventoryPartition::Page { from, to } => format!("page:{from}:{to}"),
        _ => "unknown".to_string(),
    };
    Partition(key.into_bytes())
}

fn optional_i64(value: Option<i64>) -> String {
    value.map(|v| v.to_string()).unwrap_or_default()
}

/// Plan a partition list for the given policy.
///
/// `now` is taken as a parameter so tests are deterministic. The
/// `total_for_uid_or_page` parameter is the upper bound for `UidRange`
/// (total UIDs in folder) and `PageCount` (total items in query).
#[must_use]
pub fn plan(
    policy: &BackfillPolicy,
    now: DateTime<Utc>,
    total_for_uid_or_page: u32,
) -> PartitionPlan {
    match &policy.strategy {
        BackfillStrategy::TimeWindowed { boundaries } => {
            plan_time(boundaries, now, policy.clock_skew)
        }
        BackfillStrategy::UidRange { chunk_size } => plan_uid(*chunk_size, total_for_uid_or_page),
        BackfillStrategy::PageCount {
            items_per_partition,
        } => plan_page(*items_per_partition, total_for_uid_or_page),
    }
}

fn plan_time(boundaries: &[Duration], now: DateTime<Utc>, skew: Duration) -> PartitionPlan {
    let now_skewed =
        now + chrono::Duration::from_std(skew).unwrap_or_else(|_| chrono::Duration::zero());
    let mut partitions = Vec::with_capacity(boundaries.len() + 1);
    let mut prev_to = now_skewed;
    for boundary in boundaries {
        let from = now_skewed
            - chrono::Duration::from_std(*boundary).unwrap_or_else(|_| chrono::Duration::zero());
        partitions.push(PartitionBounds::Time { from, to: prev_to });
        prev_to = from;
    }
    // Final open-ended partition: from year 1 (or DateTime::MIN_UTC) to
    // the oldest boundary. Encoded as a fixed sentinel so partition
    // identity is stable across runs.
    partitions.push(PartitionBounds::Time {
        from: DateTime::<Utc>::MIN_UTC,
        to: prev_to,
    });
    PartitionPlan { partitions }
}

fn plan_uid(chunk: u32, total: u32) -> PartitionPlan {
    if total == 0 || chunk == 0 {
        return PartitionPlan { partitions: vec![] };
    }
    let mut partitions = Vec::new();
    // Newest-first: walk high UIDs down.
    let mut to = total;
    while to > 0 {
        let from = to.saturating_sub(chunk).saturating_add(1).max(1);
        partitions.push(PartitionBounds::Uid { from, to });
        if from == 1 {
            break;
        }
        to = from.saturating_sub(1);
    }
    PartitionPlan { partitions }
}

fn plan_page(chunk: u32, total: u32) -> PartitionPlan {
    if total == 0 || chunk == 0 {
        return PartitionPlan { partitions: vec![] };
    }
    let mut partitions = Vec::new();
    let mut from: u32 = 0;
    while from < total {
        let to = from.saturating_add(chunk).min(total);
        partitions.push(PartitionBounds::Page { from, to });
        from = to;
    }
    PartitionPlan { partitions }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn now() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 5, 21, 12, 0, 0).unwrap()
    }

    #[test]
    fn time_windowed_default_partitions_in_order() {
        let policy = BackfillPolicy::default();
        let plan = plan(&policy, now(), 0);
        // 9 boundaries + 1 open-ended tail.
        assert_eq!(plan.partitions.len(), 10);
        // Newest first; each partition's `to` equals the previous
        // partition's `from`.
        for window in plan.partitions.windows(2) {
            let (
                PartitionBounds::Time { from: f0, to: _ },
                PartitionBounds::Time { from: _, to: t1 },
            ) = (&window[0], &window[1])
            else {
                panic!("expected time partitions");
            };
            assert_eq!(f0, t1, "partitions must chain back-to-back");
        }
    }

    #[test]
    fn uid_range_chunks_newest_first() {
        let policy = BackfillPolicy {
            strategy: BackfillStrategy::UidRange { chunk_size: 1000 },
            clock_skew: Duration::ZERO,
        };
        let plan = plan(&policy, now(), 2500);
        assert_eq!(
            plan.partitions,
            vec![
                PartitionBounds::Uid {
                    from: 1501,
                    to: 2500
                },
                PartitionBounds::Uid {
                    from: 501,
                    to: 1500
                },
                PartitionBounds::Uid { from: 1, to: 500 },
            ]
        );
    }

    #[test]
    fn page_count_walks_in_order() {
        let policy = BackfillPolicy {
            strategy: BackfillStrategy::PageCount {
                items_per_partition: 250,
            },
            clock_skew: Duration::ZERO,
        };
        let plan = plan(&policy, now(), 700);
        assert_eq!(
            plan.partitions,
            vec![
                PartitionBounds::Page { from: 0, to: 250 },
                PartitionBounds::Page { from: 250, to: 500 },
                PartitionBounds::Page { from: 500, to: 700 },
            ]
        );
    }

    #[test]
    fn bounds_convert_to_account_partitions() {
        let time = PartitionBounds::Time {
            from: Utc.with_ymd_and_hms(2026, 5, 1, 0, 0, 0).unwrap(),
            to: Utc.with_ymd_and_hms(2026, 5, 2, 0, 0, 0).unwrap(),
        };
        assert_eq!(
            inventory_partition_for(&time),
            InventoryPartition::Time {
                from_unix_seconds: Some(1_777_593_600),
                to_unix_seconds: Some(1_777_680_000),
            }
        );

        assert_eq!(
            inventory_partition_for(&PartitionBounds::Uid { from: 10, to: 20 }),
            InventoryPartition::Uid { from: 10, to: 20 }
        );
        assert_eq!(
            inventory_partition_for(&PartitionBounds::Page { from: 0, to: 50 }),
            InventoryPartition::Page { from: 0, to: 50 }
        );
    }

    #[test]
    fn partition_keys_are_stable() {
        assert_eq!(
            partition_key(&InventoryPartition::Page { from: 25, to: 50 }),
            Partition(b"page:25:50".to_vec())
        );
        assert_eq!(
            partition_key(&InventoryPartition::Uid { from: 7, to: 9 }),
            Partition(b"uid:7:9".to_vec())
        );
        assert_eq!(
            partition_key(&InventoryPartition::Time {
                from_unix_seconds: None,
                to_unix_seconds: Some(123),
            }),
            Partition(b"time::123".to_vec())
        );
    }

    #[test]
    fn empty_inputs_yield_empty_plan() {
        let policy = BackfillPolicy {
            strategy: BackfillStrategy::UidRange { chunk_size: 100 },
            clock_skew: Duration::ZERO,
        };
        assert!(plan(&policy, now(), 0).partitions.is_empty());
        let policy = BackfillPolicy {
            strategy: BackfillStrategy::PageCount {
                items_per_partition: 50,
            },
            clock_skew: Duration::ZERO,
        };
        assert!(plan(&policy, now(), 0).partitions.is_empty());
    }
}
