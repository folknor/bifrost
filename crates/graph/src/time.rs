use chrono::{Duration, LocalResult, NaiveDateTime, TimeZone};

pub(crate) fn resolve_local_to_timestamp<Tz: TimeZone>(
    naive: NaiveDateTime,
    tz: &Tz,
) -> Option<i64> {
    match tz.from_local_datetime(&naive) {
        LocalResult::Single(t) => Some(t.timestamp()),
        LocalResult::Ambiguous(early, _late) => Some(early.timestamp()),
        LocalResult::None => resolve_through_gap(naive, tz),
    }
}

fn resolve_through_gap<Tz: TimeZone>(naive: NaiveDateTime, tz: &Tz) -> Option<i64> {
    const MAX_PROBE_MINUTES: i64 = 60 * 48;

    let mut backward = 0i64;
    let gap_start_offset = loop {
        backward += 1;
        if backward > MAX_PROBE_MINUTES {
            return None;
        }
        if !matches!(
            tz.from_local_datetime(&(naive - Duration::minutes(backward))),
            LocalResult::None
        ) {
            break backward;
        }
    };

    let mut forward = 0i64;
    let gap_end_offset = loop {
        forward += 1;
        if forward > MAX_PROBE_MINUTES {
            return None;
        }
        if !matches!(
            tz.from_local_datetime(&(naive + Duration::minutes(forward))),
            LocalResult::None
        ) {
            break forward;
        }
    };

    let gap_width = gap_start_offset + gap_end_offset - 1;
    let shifted = naive.checked_add_signed(Duration::minutes(gap_width))?;
    match tz.from_local_datetime(&shifted) {
        LocalResult::Single(t) => Some(t.timestamp()),
        LocalResult::Ambiguous(_early, late) => Some(late.timestamp()),
        LocalResult::None => None,
    }
}
