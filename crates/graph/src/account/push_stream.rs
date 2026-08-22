use bifrost_types::{AccountStream, HintPayload, InvalidationHint, PushSource, WatchEvent};
use futures::stream;

use super::{GraphAccount, PushMode};

/// A broadcast overflow means invalidations were dropped and can never
/// be replayed: the events themselves are the only record that those
/// scopes changed. Swallowing the lag leaves the affected folders
/// waiting for the ordinary poll, so synthesize a coalesced
/// whole-account invalidation instead and let the engine reconcile.
fn coalesced_overflow_event() -> WatchEvent {
    WatchEvent::Invalidated {
        hint: InvalidationHint {
            source: PushSource::Coalesced,
            payload: HintPayload::Unknown,
        },
    }
}

pub(crate) fn push_stream(account: GraphAccount) -> AccountStream<WatchEvent> {
    let receiver = account.push_tx.subscribe();
    let shutdown = account.shutdown.clone();
    Box::pin(stream::unfold(
        (receiver, shutdown),
        |(mut receiver, shutdown)| async move {
            tokio::select! {
                () = shutdown.cancelled() => None,
                result = receiver.recv() => {
                    match result {
                        Ok(event) => Some((event, (receiver, shutdown))),
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(dropped)) => {
                            tracing::warn!(
                                dropped,
                                "graph push channel lagged; synthesizing a coalesced invalidation"
                            );
                            Some((coalesced_overflow_event(), (receiver, shutdown)))
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => None,
                    }
                }
            }
        },
    ))
}

pub(crate) async fn ensure_ews_worker(account: GraphAccount) {
    if account.push_mode != PushMode::EwsStreaming {
        return;
    }
    let worker_account = account.clone();
    super::worker_slot::ensure_worker(&account.ews_worker, move || {
        tokio::spawn(async move {
            super::ews_stream::run_streaming_worker(worker_account).await;
        })
    })
    .await;
}
