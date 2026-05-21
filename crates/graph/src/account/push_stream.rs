use bifrost_types::{AccountStream, WatchEvent};
use futures_util::stream;

use super::{GraphAccount, PushMode};

pub(crate) fn push_stream(account: GraphAccount) -> AccountStream<WatchEvent> {
    match account.push_mode {
        PushMode::GraphSubscriptions => Box::pin(stream::empty()),
        PushMode::EwsStreaming => {
            let receiver = account.push_tx.subscribe();
            Box::pin(stream::unfold(receiver, |mut receiver| async move {
                loop {
                    match receiver.recv().await {
                        Ok(event) => return Some((event, receiver)),
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => return None,
                    }
                }
            }))
        }
    }
}

pub(crate) async fn ensure_ews_worker(account: GraphAccount) {
    if account.push_mode != PushMode::EwsStreaming {
        return;
    }
    let mut worker = account.ews_worker.lock().await;
    let needs_start = worker
        .as_ref()
        .is_none_or(tokio::task::JoinHandle::is_finished);
    if needs_start {
        let worker_account = account.clone();
        *worker = Some(tokio::spawn(async move {
            super::ews_stream::run_streaming_worker(worker_account).await;
        }));
    }
}
