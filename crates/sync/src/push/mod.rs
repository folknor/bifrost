//! Push reconciler + `InvalidationSink` implementation.
//!
//! Two push paths converge on one per-account `mpsc::Sender<WatchEvent>`:
//! - In-process: the multiplexer forwards `Account::push_stream` items.
//! - Out-of-process: the consumer holds an `Arc<dyn InvalidationSink>`
//!   (from `SyncEngine::invalidation_sink`) and calls `push` from its
//!   Pub/Sub or webhook receiver.
//!
//! The reconciler reads the merged stream and runs `changes_stream`
//! for each affected scope via `drive_to_completion`. Output flows to
//! the same broadcast as the multiplexer's, so consumers do not see
//! "push-derived" vs "poll-derived" changes - just one unified
//! `Change` stream.

pub mod reconciler;
pub mod subscription;

use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};

use bifrost_types::{
    AccountId, HintPayload, InvalidationHint, InvalidationSink, PushSource, WatchEvent,
};
use dashmap::DashMap;
use tokio::sync::mpsc;

pub use reconciler::{Reconciler, scopes_for_hint};
pub(crate) use subscription::RegisteredSubscription;
pub use subscription::SubscriptionRegistry;

/// Engine-side `InvalidationSink` implementation.
///
/// `senders` is the registry of per-account mpsc senders. `attach`
/// inserts; `detach` removes. The drop counter is exposed as
/// `bifrost_sync_push_dropped_total`.
#[derive(Debug)]
pub struct InvalidationSinkInner {
    senders: DashMap<AccountId, mpsc::Sender<WatchEvent>>,
    drop_counter: AtomicU64,
    runtime: OnceLock<tokio::runtime::Handle>,
}

impl InvalidationSinkInner {
    #[must_use]
    pub fn new() -> Self {
        Self {
            senders: DashMap::new(),
            drop_counter: AtomicU64::new(0),
            runtime: OnceLock::new(),
        }
    }

    pub fn register(&self, account: AccountId, sender: mpsc::Sender<WatchEvent>) {
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            let _ = self.runtime.set(handle);
        }
        self.senders.insert(account, sender);
    }

    pub fn unregister(&self, account: &AccountId) {
        self.senders.remove(account);
    }

    #[must_use]
    pub fn dropped(&self) -> u64 {
        self.drop_counter.load(Ordering::Relaxed)
    }
}

impl Default for InvalidationSinkInner {
    fn default() -> Self {
        Self::new()
    }
}

impl InvalidationSink for InvalidationSinkInner {
    fn push(&self, account: AccountId, event: WatchEvent) {
        let Some(tx) = self.senders.get(&account) else {
            return;
        };
        match tx.try_send(event) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(rejected)) => {
                let sender = tx.clone();
                let lossless = requires_lossless_delivery(&rejected);
                if let Some(handle) = self.runtime.get() {
                    // Only the lossy coalescing lane counts as a drop: the
                    // lossless lane below waits for queue space and delivers
                    // the original event.
                    if !lossless {
                        self.drop_counter.fetch_add(1, Ordering::Relaxed);
                    }
                    let delivery = if lossless {
                        rejected
                    } else {
                        coalesced_event(rejected)
                    };
                    handle.spawn(async move {
                        if lossless {
                            // Terminal classifications and warnings
                            // are control information, not redundant
                            // invalidation hints. Preserve the exact
                            // event and wait for queue space.
                            let _ = sender.send(delivery).await;
                        } else {
                            // Invalidations and health transitions can
                            // collapse to one bounded full reconcile.
                            let deadline = tokio::time::Duration::from_millis(100);
                            let _ = tokio::time::timeout(deadline, sender.send(delivery)).await;
                        }
                    });
                } else {
                    // No runtime handle was ever captured (`register` ran
                    // outside Tokio), so nothing can wait for queue space:
                    // the event is discarded regardless of classification.
                    // A lossless discard is still a drop - leaving it
                    // uncounted would hide the loss of control information
                    // from the one signal built to expose it.
                    self.drop_counter.fetch_add(1, Ordering::Relaxed);
                }
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                // Account detached during in-flight push.
            }
        }
    }
}

#[must_use]
pub(crate) fn requires_lossless_delivery(event: &WatchEvent) -> bool {
    matches!(event, WatchEvent::Terminated(_) | WatchEvent::Warning(_))
}

pub(crate) fn coalesced_event(event: WatchEvent) -> WatchEvent {
    let source = match event {
        WatchEvent::Invalidated { hint } => hint.source,
        WatchEvent::Disconnected | WatchEvent::Reconnected => PushSource::Coalesced,
        _ => PushSource::Coalesced,
    };
    WatchEvent::Invalidated {
        hint: InvalidationHint {
            source,
            payload: HintPayload::Unknown,
        },
    }
}

/// Convenience: wrap in an `Arc`.
#[must_use]
pub fn shared_sink() -> Arc<InvalidationSinkInner> {
    Arc::new(InvalidationSinkInner::new())
}

#[cfg(test)]
mod tests {
    use super::*;
    use bifrost_types::{AccountOperation, CursorScope, Warning, WarningKind};

    fn invalidated() -> WatchEvent {
        WatchEvent::Invalidated {
            hint: InvalidationHint {
                source: PushSource::ImapNotify,
                payload: HintPayload::Unknown,
            },
        }
    }

    #[tokio::test]
    async fn full_sink_preserves_warning_from_a_non_runtime_thread() {
        let sink = Arc::new(InvalidationSinkInner::new());
        let account = AccountId("warning".into());
        let (tx, mut rx) = mpsc::channel(1);
        sink.register(account.clone(), tx);
        sink.senders
            .get(&account)
            .expect("registered")
            .try_send(invalidated())
            .expect("fill queue");

        let outside = Arc::clone(&sink);
        std::thread::spawn(move || {
            outside.push(
                account,
                WatchEvent::Warning(Warning::user_safe(WarningKind::Other, "preserve me")),
            );
        })
        .join()
        .expect("push thread");

        assert!(matches!(
            rx.recv().await,
            Some(WatchEvent::Invalidated { .. })
        ));
        let delivered = rx.recv().await;
        assert!(matches!(delivered, Some(WatchEvent::Warning(_))));
        assert_eq!(sink.dropped(), 0, "a delivered warning was not dropped");
    }

    #[tokio::test]
    async fn full_sink_preserves_terminated_classification() {
        let sink = Arc::new(InvalidationSinkInner::new());
        let account = AccountId("terminated".into());
        let (tx, mut rx) = mpsc::channel(1);
        sink.register(account.clone(), tx);
        sink.senders
            .get(&account)
            .expect("registered")
            .try_send(invalidated())
            .expect("fill queue");
        let error = crate::recovery::restart_scope_error(
            CursorScope::Account,
            AccountOperation::SyncChanges,
        );

        sink.push(account, WatchEvent::Terminated(error));

        assert!(matches!(
            rx.recv().await,
            Some(WatchEvent::Invalidated { .. })
        ));
        let delivered = rx.recv().await;
        assert!(matches!(delivered, Some(WatchEvent::Terminated(_))));
        assert_eq!(sink.dropped(), 0, "a delivered termination was not dropped");
    }

    #[tokio::test]
    async fn full_sink_counts_a_coalesced_invalidation_as_dropped() {
        let sink = Arc::new(InvalidationSinkInner::new());
        let account = AccountId("coalesced".into());
        let (tx, mut rx) = mpsc::channel(1);
        sink.register(account.clone(), tx);
        sink.senders
            .get(&account)
            .expect("registered")
            .try_send(invalidated())
            .expect("fill queue");

        sink.push(account, invalidated());

        assert_eq!(sink.dropped(), 1, "the replaced invalidation was dropped");
        assert!(matches!(
            rx.recv().await,
            Some(WatchEvent::Invalidated { .. })
        ));
        assert!(matches!(
            rx.recv().await,
            Some(WatchEvent::Invalidated { .. })
        ));
    }

    /// With no captured runtime handle nothing can wait for queue space, so
    /// even a lossless event hitting a full queue is discarded - and a
    /// discard the metric does not count is control information lost with no
    /// trace. This test runs entirely off-runtime so `register` never
    /// captures a handle.
    #[test]
    fn no_runtime_discard_of_a_lossless_event_is_counted() {
        let sink = Arc::new(InvalidationSinkInner::new());
        let account = AccountId("no-runtime".into());
        let (tx, mut rx) = mpsc::channel(1);
        sink.register(account.clone(), tx);
        sink.senders
            .get(&account)
            .expect("registered")
            .try_send(invalidated())
            .expect("fill queue");

        sink.push(
            account,
            WatchEvent::Warning(Warning::user_safe(WarningKind::Other, "discarded")),
        );

        assert_eq!(sink.dropped(), 1, "the discarded warning must be counted");
        assert!(matches!(rx.try_recv(), Ok(WatchEvent::Invalidated { .. })));
        assert!(
            rx.try_recv().is_err(),
            "nothing could redeliver the warning"
        );
    }

    #[test]
    fn only_control_information_requires_lossless_delivery() {
        let warning = WatchEvent::Warning(Warning::user_safe(WarningKind::Other, "warning"));
        let terminated = WatchEvent::Terminated(crate::recovery::restart_scope_error(
            CursorScope::Account,
            AccountOperation::SyncChanges,
        ));
        assert!(requires_lossless_delivery(&warning));
        assert!(requires_lossless_delivery(&terminated));
        assert!(!requires_lossless_delivery(&invalidated()));
        assert!(!requires_lossless_delivery(&WatchEvent::Disconnected));
        assert!(!requires_lossless_delivery(&WatchEvent::Reconnected));
    }
}
