//! Adaptive poll cadence.
//!
//! Each non-IDLE scope has its own poll interval that halves on
//! observed change (down to a min) and doubles after five consecutive
//! no-change ticks (up to a max). The state lives in
//! `HashMap<CursorScope, AdaptiveCadence>` on the multiplexer.

use std::time::Duration;

use bifrost_types::CursorScope;

#[derive(Debug, Clone, Copy)]
pub struct AdaptiveCadence {
    pub interval: Duration,
    pub no_change_streak: u32,
}

impl Default for AdaptiveCadence {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(60),
            no_change_streak: 0,
        }
    }
}

/// One round-robin slot.
#[derive(Debug, Clone)]
pub struct PollSchedule {
    pub scope: CursorScope,
    pub cadence: AdaptiveCadence,
}

impl PollSchedule {
    #[must_use]
    pub fn new(scope: CursorScope, initial: Duration) -> Self {
        Self {
            scope,
            cadence: AdaptiveCadence {
                interval: initial,
                no_change_streak: 0,
            },
        }
    }
}
