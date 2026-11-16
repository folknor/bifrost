//! Account-owned effects survive session cancellation.
use std::sync::{Arc, Mutex};

use futures::FutureExt;
use futures::future::{BoxFuture, Shared};

type TrackedTask = Shared<BoxFuture<'static, ()>>;

/// One shared task registry across all metered views of an opened account.
///
/// Entries are shared wrappers over each task's `JoinHandle`, and `join`
/// clones them rather than taking them out. Concurrent closes therefore all
/// wait on the same cleanup tasks, and a close future dropped mid-join
/// detaches nothing another close is still waiting on: an enterprise client
/// that awaits any close before exiting must not exit while a sibling close
/// alone holds the EWS releases.
#[derive(Clone, Default)]
pub(crate) struct PushTasks(Arc<Mutex<Vec<TrackedTask>>>);
impl PushTasks {
    pub(crate) fn track(&self, task: tokio::task::JoinHandle<()>) {
        let task = async move {
            if let Err(error) = task.await {
                tracing::warn!(target: "bifrost_graph::push", %error, "push cleanup task failed");
            }
        }
        .boxed()
        .shared();
        let mut tasks = self.0.lock().expect("push tasks lock");
        tasks.retain(|t| t.peek().is_none());
        tasks.push(task);
    }
    /// Wait for every task tracked at the time of the call, under one
    /// bound. A later `track` is collected by a later join, not this one.
    pub(crate) async fn join(&self) {
        let tasks = self.0.lock().expect("push tasks lock").clone();
        // One join bound. A task still running at the bound keeps running:
        // dropping a handle detaches the effect, and aborting would strand
        // the server-side cleanup it owns.
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
        let mut left_running = 0_usize;
        for task in tasks {
            if tokio::time::timeout_at(deadline, task).await.is_err() {
                left_running += 1;
            }
        }
        if left_running > 0 {
            tracing::warn!(target: "bifrost_graph::push", left_running, "push cleanup tasks outlived the close() budget; left running detached");
        }
        self.0
            .lock()
            .expect("push tasks lock")
            .retain(|t| t.peek().is_none());
    }
}
pub(crate) fn track(account: &super::GraphAccount, task: tokio::task::JoinHandle<()>) {
    account.push_tasks.track(task);
}

#[cfg(test)]
mod tests {
    use super::PushTasks;

    /// A second join waits on the same task the first is waiting on, and
    /// dropping the first waiter does not let the second return early.
    #[tokio::test]
    async fn concurrent_joins_share_the_tracked_tasks() {
        let tasks = PushTasks::default();
        let (release, gate) = tokio::sync::oneshot::channel::<()>();
        tasks.track(tokio::spawn(async move {
            let _ = gate.await;
        }));

        let mut first = Box::pin(tasks.join());
        assert!(futures::poll!(&mut first).is_pending());
        let mut second = Box::pin(tasks.join());
        assert!(
            futures::poll!(&mut second).is_pending(),
            "a concurrent join must wait for the task the first join holds",
        );
        drop(first);
        tokio::task::yield_now().await;
        assert!(futures::poll!(&mut second).is_pending());

        release.send(()).unwrap();
        second.await;
        assert!(tasks.0.lock().unwrap().is_empty());
    }
}
