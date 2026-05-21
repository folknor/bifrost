//! Engine-internal types: configuration knobs, work-kind tags, and
//! the `AccountSlot` shape the engine keeps in its `DashMap`.

use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;
use bifrost_types::{Account, AccountCapabilities, AccountFactory, Priority};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::backfill::BackfillHandle;
use crate::cancel::BoundaryRequest;
use crate::control::SyncControl;
use crate::cursor::CursorRegistry;
use crate::cursor::store::DynCheckpointStore;
use crate::multiplexer::MultiplexerHandle;
use crate::mutation::MutationHandle;
use crate::push::PushHandle;
use crate::scheduler::budget::ConcurrencyBudget;

/// Top-level engine configuration. Cloned into every `AccountSlot`.
#[derive(Debug, Clone)]
pub struct EngineConfig {
    pub budget: ConcurrencyBudget,
    pub scheduler: SchedulerConfig,
    pub multiplexer: MultiplexerConfig,
    pub backfill: BackfillConfig,
    pub push: PushConfig,
    pub mutation: MutationConfig,
    /// Detach / drop timeout for awaiting spawned workers (see `H1`).
    pub detach_timeout: Duration,
    /// Per-lane cap on the scheduler's `VecDeque`. On overflow the
    /// oldest item is shed and a counter incremented (see `H4`).
    pub lane_capacity: usize,
    /// Mutation campaign retry cap on `RecoveryClass::Retry`.
    pub mutation_max_retries: u32,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            budget: ConcurrencyBudget::default(),
            scheduler: SchedulerConfig::default(),
            multiplexer: MultiplexerConfig::default(),
            backfill: BackfillConfig::default(),
            push: PushConfig::default(),
            mutation: MutationConfig::default(),
            detach_timeout: Duration::from_secs(5),
            lane_capacity: 1024,
            mutation_max_retries: 5,
        }
    }
}

/// Scheduler tuning knobs.
#[derive(Debug, Clone, Copy)]
pub struct SchedulerConfig {
    /// Number of consecutive higher-lane pulls before the starvation
    /// floor forces one lower-lane pull. Default 64 per
    /// `bifrost-sync.md` -> Starvation guards.
    pub starvation_floor: u32,
}

impl Default for SchedulerConfig {
    fn default() -> Self {
        Self {
            starvation_floor: 64,
        }
    }
}

/// Multiplexer cadence and reopen policy.
#[derive(Debug, Clone, Copy)]
pub struct MultiplexerConfig {
    /// Initial NOOP/STATUS poll interval for non-IDLE scopes.
    pub poll_initial: Duration,
    /// Floor on the adaptive cadence (interval halves on change-seen
    /// down to this minimum).
    pub poll_min: Duration,
    /// Ceiling on the adaptive cadence (interval doubles on five
    /// consecutive no-change ticks up to this maximum).
    pub poll_max: Duration,
    /// Bound on the per-account `WatchEvent` buffer fed by the push
    /// reconciler.
    pub watch_capacity: usize,
    /// Bound on the per-account broadcast carrying the unified
    /// `account_changes_stream` payload.
    pub changes_capacity: usize,
}

impl Default for MultiplexerConfig {
    fn default() -> Self {
        Self {
            poll_initial: Duration::from_secs(60),
            poll_min: Duration::from_secs(30),
            poll_max: Duration::from_secs(30 * 60),
            watch_capacity: 256,
            changes_capacity: 256,
        }
    }
}

/// Backfill partitioner / runner config.
#[derive(Debug, Clone, Copy)]
pub struct BackfillConfig {
    /// IMAP-on-Basic fallback chunk size for `UidRange` strategy.
    pub uid_range_chunk: u32,
    /// JMAP `PageCount` strategy page size.
    pub page_count_chunk: u32,
    /// Clock-skew threshold above which a `Warning::ClockSkew` is
    /// emitted.
    pub clock_skew_warn: Duration,
}

impl Default for BackfillConfig {
    fn default() -> Self {
        Self {
            uid_range_chunk: 5000,
            page_count_chunk: 1000,
            clock_skew_warn: Duration::from_secs(5 * 60),
        }
    }
}

/// Push reconciler config.
///
/// Capacity for the per-account `WatchEvent` mpsc lives on
/// `MultiplexerConfig::watch_capacity`; this struct is reserved for
/// future push-only knobs.
#[derive(Debug, Clone, Copy, Default)]
pub struct PushConfig {}

/// Mutation pipeline config.
#[derive(Debug, Clone, Copy)]
pub struct MutationConfig {
    /// Default fan-out per-account sub-channel buffer.
    pub fanout_buffer: usize,
    /// Default per-campaign retry queue limit.
    pub retry_queue_cap: usize,
}

impl Default for MutationConfig {
    fn default() -> Self {
        Self {
            fanout_buffer: 256,
            retry_queue_cap: 4096,
        }
    }
}

/// What kind of work is requesting a permit. Used by the budget gate
/// to pick between the mutation sub-pool and the general pool.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkKind {
    /// Changes, inventory, backfill, reconcile.
    Sync,
    /// Mutations (bulk_set_flags / move / destroy).
    Mutation,
}

/// Engine-side per-account state. Held inside the engine's
/// `DashMap<AccountId, Arc<AccountSlot>>`.
///
/// `current` is an `ArcSwap<Arc<dyn Account>>` so a reopen swap
/// becomes visible to spawned workers (reconciler, multiplexer,
/// backfill, mutation) at their next iteration without re-spawning.
/// Workers load with `current.load_full()`.
#[allow(dead_code)]
pub(crate) struct AccountSlot {
    pub factory: Arc<dyn AccountFactory>,
    pub current: Arc<ArcSwap<Arc<dyn Account>>>,
    pub capabilities: AccountCapabilities,
    pub multiplexer: MultiplexerHandle,
    pub backfill: BackfillHandle,
    pub push: PushHandle,
    pub mutation: MutationHandle,
    pub cursors: Arc<CursorRegistry>,
    pub checkpoints: Arc<DynCheckpointStore>,
    pub priority_tx: watch::Sender<Priority>,
    pub boundary_tx: watch::Sender<BoundaryRequest>,
    pub shutdown: CancellationToken,
    pub control: SyncControl,
    /// Sentinel receiver keeps `changes_tx` alive across periods with
    /// no subscribers so new subscribers don't get a closed-channel
    /// error.
    pub _sentinel_rx: tokio::sync::broadcast::Receiver<crate::multiplexer::MultiplexerEvent>,
    /// `JoinHandle`s for every worker spawned in `attach`, in spawn
    /// order: reconciler, push forwarder (optional), multiplexer,
    /// backfill orchestrator. `detach` awaits these up to
    /// `EngineConfig::detach_timeout`.
    pub workers: tokio::sync::Mutex<Vec<JoinHandle<()>>>,
}
