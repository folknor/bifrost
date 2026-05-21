//! Safe-boundary primitive.
//!
//! The engine drives long streams through workers that emit `Batch`s
//! with checkpoints. Pause / checkpoint_now / hard-stop semantics
//! depend on the worker stopping cleanly at a `Batch` boundary - never
//! mid-page - so the checkpoint persists alongside the data.
//!
//! `BoundaryRequest` is a `tokio::sync::watch` value workers peek
//! before pulling the next batch from their underlying stream. The
//! engine flips the request from `Run` to `Pause` / `CheckpointNow` /
//! `Stop` via the `Control` surface; the worker reacts at the next
//! safe boundary.

use tokio::sync::watch;

/// Per-task safe-boundary request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum BoundaryRequest {
    /// Default state. Worker proceeds.
    Run,
    /// Persist the next batch's checkpoint, then park until the
    /// request changes back to `Run` or to `Stop`.
    Pause,
    /// Persist the next batch's checkpoint, emit `Done`, then flip
    /// back to `Run` so the worker can pick up subsequent work.
    CheckpointNow,
    /// Persist the most recent checkpoint, emit `Done`, exit the
    /// worker.
    Stop,
}

/// Engine-side handle for flipping the boundary request.
#[derive(Debug, Clone)]
pub struct Boundary {
    tx: watch::Sender<BoundaryRequest>,
}

impl Boundary {
    #[must_use]
    pub fn new() -> (Self, BoundaryView) {
        let (tx, rx) = watch::channel(BoundaryRequest::Run);
        (Self { tx }, BoundaryView { rx })
    }

    pub fn set(&self, req: BoundaryRequest) {
        // Errors here mean every receiver has dropped; that is the
        // shutdown path and not an engine bug.
        let _ = self.tx.send(req);
    }

    #[must_use]
    pub fn snapshot(&self) -> BoundaryRequest {
        *self.tx.borrow()
    }

    #[must_use]
    pub fn sender(&self) -> watch::Sender<BoundaryRequest> {
        self.tx.clone()
    }

    pub fn subscribe(&self) -> BoundaryView {
        BoundaryView {
            rx: self.tx.subscribe(),
        }
    }
}

impl Default for Boundary {
    fn default() -> Self {
        Self::new().0
    }
}

impl From<watch::Sender<BoundaryRequest>> for Boundary {
    fn from(tx: watch::Sender<BoundaryRequest>) -> Self {
        Self { tx }
    }
}

/// Per-worker view of the boundary. Cheaply cloneable.
#[derive(Debug, Clone)]
pub struct BoundaryView {
    rx: watch::Receiver<BoundaryRequest>,
}

impl BoundaryView {
    /// Cheap, non-blocking read of the current request. Workers call
    /// this at the top of each batch iteration.
    #[must_use]
    pub fn peek(&self) -> BoundaryRequest {
        *self.rx.borrow()
    }

    /// Park until the request changes. Returns the new request or
    /// `None` if every sender has dropped (engine teardown).
    pub async fn changed(&mut self) -> Option<BoundaryRequest> {
        if self.rx.changed().await.is_err() {
            return None;
        }
        Some(*self.rx.borrow())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_request_is_run() {
        let (b, v) = Boundary::new();
        assert_eq!(b.snapshot(), BoundaryRequest::Run);
        assert_eq!(v.peek(), BoundaryRequest::Run);
    }

    #[test]
    fn set_propagates() {
        let (b, v) = Boundary::new();
        b.set(BoundaryRequest::Pause);
        assert_eq!(v.peek(), BoundaryRequest::Pause);
        b.set(BoundaryRequest::Stop);
        assert_eq!(v.peek(), BoundaryRequest::Stop);
    }
}
