//! Engine-internal types: configuration knobs, work-kind tags, and
//! the `AccountSlot` shape the engine keeps in its `DashMap`.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use arc_swap::ArcSwap;
use bifrost_types::{Account, AccountCapabilities, AccountControl, AccountFactory};
use tokio::sync::{Notify, broadcast, mpsc, watch};

use crate::multiplexer::ReopenRequest;
use tokio::task::{AbortHandle, JoinHandle};
use tokio_util::sync::CancellationToken;

use crate::cancel::BoundaryRequest;
use crate::control::SyncControl;
use crate::cursor::CursorRegistry;
use crate::cursor::store::DynCheckpointStore;
use crate::multiplexer::MultiplexerHandle;

/// Top-level engine configuration. Cloned into every `AccountSlot`.
#[derive(Debug, Clone)]
pub struct EngineConfig {
    pub multiplexer: MultiplexerConfig,
    pub backfill: BackfillConfig,
    pub push: PushConfig,
    /// Detach / drop timeout for awaiting spawned workers (see `H1`).
    pub detach_timeout: Duration,
    /// Mutation campaign retry cap. The campaign attempts up to this
    /// many resubmissions for ids whose per-item `ItemOutcome::Failed`
    /// carried a retryable `RecoveryClass`.
    pub mutation_max_retries: u32,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            multiplexer: MultiplexerConfig::default(),
            backfill: BackfillConfig::default(),
            push: PushConfig::default(),
            detach_timeout: Duration::from_secs(5),
            mutation_max_retries: 5,
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
}

impl Default for BackfillConfig {
    fn default() -> Self {
        Self {
            uid_range_chunk: 5000,
            page_count_chunk: 1000,
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

/// Engine-side per-account state. Held inside the engine's
/// `DashMap<AccountId, Arc<AccountSlot>>`.
///
/// `current` is an `ArcSwap<Arc<dyn Account>>` so a reopen swap
/// becomes visible to spawned workers (reconciler, multiplexer,
/// backfill, mutation) at their next iteration without re-spawning.
/// Workers load with `current.load_full()`.
pub(crate) struct AccountSlot {
    pub factory: Arc<dyn AccountFactory>,
    pub current: Arc<ArcSwap<Arc<dyn Account>>>,
    /// Monotonic signal bumped after a successful handle swap. Long-
    /// lived push and lifecycle streams select on it so they drop the
    /// old stream even when that stream never terminates itself.
    pub account_generation_tx: watch::Sender<u64>,
    pub reopen_lock: Arc<tokio::sync::Mutex<()>>,
    pub capabilities: Arc<std::sync::RwLock<AccountCapabilities>>,
    pub multiplexer: MultiplexerHandle,
    pub cursors: Arc<CursorRegistry>,
    pub checkpoints: Arc<DynCheckpointStore>,
    pub boundary_tx: watch::Sender<BoundaryRequest>,
    pub shutdown: CancellationToken,
    pub control: SyncControl,
    /// Sentinel receiver keeps `changes_tx` alive across periods with
    /// no subscribers so new subscribers don't get a closed-channel
    /// error.
    #[allow(dead_code)]
    pub _sentinel_rx: tokio::sync::broadcast::Receiver<crate::multiplexer::MultiplexerEvent>,
    /// Spawned workers paired with their abort handles. `detach`
    /// aborts each worker on timeout so tasks do not leak past the
    /// configured deadline.
    pub workers: Mutex<Vec<WorkerTask>>,
    /// Per-account control stream. Engine publishes
    /// `AccountControl::Pause(reason)` when the engine has automatically
    /// paused the account (operator-override directive, retry budget
    /// exhausted, tenant throttle). Consumers subscribe via
    /// `SyncEngine::account_control_stream`.
    pub account_control_tx: broadcast::Sender<AccountControl>,
    /// Sentinel receiver keeps the account-control channel alive.
    #[allow(dead_code)]
    pub _account_control_sentinel: broadcast::Receiver<AccountControl>,
    /// Notify fired when a new real subscriber arrives on
    /// `changes_tx`. Lets deferred-inventory workers park on `notified`
    /// instead of hot-polling `receiver_count`.
    pub subscriber_notify: Arc<Notify>,
    /// Per-account reopen channel. The bulk-mutation campaign routes
    /// `EngineDirective::*` raised mid-stream through here so the
    /// reopen listener dispatches via `handle_engine_directive`.
    pub reopen_tx: mpsc::Sender<ReopenRequest>,
    /// Engine-wide throttle bucket (shared by every slot so tenant /
    /// provider deadlines cross accounts). Recovery paths record waits
    /// keyed by `ThrottleKey`; the per-scope poll loop and the push
    /// reconciler consult it before driving work.
    pub throttles: Arc<std::sync::Mutex<crate::recovery::ThrottleBucket>>,
    /// The `OpenedAccount::skipped_scopes` lane from the most recent
    /// successful `factory.open` (attach or reopen swap): the parts of
    /// the account surface the protocol crate discovered but could not
    /// bring up, with their classified errors. Exposed via
    /// `SyncEngine::open_skipped_scopes` so a consumer can tell a
    /// degraded shared namespace apart from one that does not exist.
    pub open_skips: Arc<Mutex<Vec<bifrost_types::SkippedScope>>>,
}

pub(crate) struct WorkerTask {
    pub join: JoinHandle<()>,
    pub abort: AbortHandle,
}
