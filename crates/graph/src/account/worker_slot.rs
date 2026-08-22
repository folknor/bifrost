use std::sync::Arc;

use tokio::sync::Mutex;
use tokio::task::JoinHandle;

/// The single background task backing one push mode, plus the rules that
/// keep "is there live work" and "is there a worker" from drifting apart.
///
/// Both push modes have the same lifecycle: a registration installs local
/// state and then ensures a worker exists; the worker exits by itself once
/// the last registration is gone. Written naively that is a race - the
/// worker samples an empty registration map, a concurrent registration
/// lands, its `ensure` sees the still-unfinished `JoinHandle` and declines
/// to spawn, and then the old worker returns. The new registration is left
/// with no worker at all and push is silently dead.
///
/// The fix is an ordering both modes obey:
///
/// - a registration writes its state under the registration lock, drops
///   that lock, and only then calls [`ensure_worker`];
/// - a worker that decides to exit calls [`retire_worker_slot`] while it
///   still holds the registration guard that made the decision.
///
/// A concurrent registration therefore cannot install until the exiting
/// worker has emptied the slot, so its `ensure_worker` always spawns a
/// replacement. The lock order is registration-then-worker in both modes,
/// and `ensure_worker` takes only the worker lock, so the pair cannot
/// deadlock.
pub(crate) type WorkerSlot = Arc<Mutex<Option<JoinHandle<()>>>>;

pub(crate) fn worker_slot() -> WorkerSlot {
    Arc::new(Mutex::new(None))
}

/// Spawn `spawn()` into `slot` unless a live worker already occupies it.
///
/// A finished `JoinHandle` counts as empty: the worker retired itself and
/// nothing else claimed the slot.
pub(crate) async fn ensure_worker<F>(slot: &WorkerSlot, spawn: F)
where
    F: FnOnce() -> JoinHandle<()>,
{
    let mut worker = slot.lock().await;
    let needs_start = worker
        .as_ref()
        .is_none_or(tokio::task::JoinHandle::is_finished);
    if needs_start {
        *worker = Some(spawn());
    }
}

/// Clear the slot on the way out of the worker task itself.
///
/// Dropping whatever is in the slot is safe without identifying the handle:
/// [`ensure_worker`] installs one only when the slot is empty or its task
/// has already finished, and the caller here is neither, so the slot holds
/// either this task's own handle or `None` (a teardown path took it to
/// abort us). Dropping a `JoinHandle` detaches, it does not cancel.
pub(crate) async fn retire_worker_slot(slot: &WorkerSlot) {
    drop(slot.lock().await.take());
}

/// Take the slot's handle for a teardown path that wants to abort or join
/// the worker itself.
pub(crate) async fn take_worker(slot: &WorkerSlot) -> Option<JoinHandle<()>> {
    slot.lock().await.take()
}
