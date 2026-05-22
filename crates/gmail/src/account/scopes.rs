use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use bifrost_types::{
    AccountStream, Batch, CursorScope, LabelId, MembershipScope, PageBoundary, RecoveryClass,
    ScopeLifecycle, SyncEvent,
};
use futures::{StreamExt, stream};
use tokio_util::sync::CancellationToken;

use crate::client::GmailClient;
use crate::types::GmailLabel;

use super::recovery;

const LIFECYCLE_POLL_INTERVAL: Duration = Duration::from_secs(30);
pub(crate) const SCOPE_CACHE_STALE_AFTER: Duration = Duration::from_secs(300);

#[derive(Debug, Clone)]
pub(crate) struct ScopeSnapshot {
    pub labels: Vec<GmailLabel>,
    pub fetched_at: Instant,
}

impl ScopeSnapshot {
    pub(crate) fn empty() -> Self {
        Self {
            labels: Vec::new(),
            fetched_at: Instant::now(),
        }
    }

    pub(crate) fn is_stale(&self) -> bool {
        self.fetched_at.elapsed() > SCOPE_CACHE_STALE_AFTER
    }
}

pub(crate) type ScopeCache = Arc<RwLock<ScopeSnapshot>>;

struct LifecycleState {
    client: Arc<GmailClient>,
    cache: ScopeCache,
    shutdown: CancellationToken,
    pending: VecDeque<ScopeLifecycle>,
    initialized: bool,
}

pub(crate) fn discover_cursor_scopes() -> AccountStream<SyncEvent<CursorScope>> {
    let batch = Batch {
        items: vec![CursorScope::Account],
        page_boundary: PageBoundary::Final,
        server_latency: Duration::ZERO,
        bytes_in: 0,
        checkpoint: None,
    };
    Box::pin(stream::iter([
        SyncEvent::Batch(batch),
        SyncEvent::Done(None),
    ]))
}

pub(crate) fn discover_memberships(
    client: Arc<GmailClient>,
    cache: ScopeCache,
) -> AccountStream<SyncEvent<MembershipScope>> {
    Box::pin(
        stream::once(async move {
            let started = Instant::now();
            match refresh_scope_snapshot(&client, &cache).await {
                Ok(snapshot) => {
                    let items = snapshot
                        .labels
                        .iter()
                        .map(|label| MembershipScope::Label(LabelId(label.id.clone())))
                        .collect();
                    SyncEvent::Batch(Batch {
                        items,
                        page_boundary: PageBoundary::Final,
                        server_latency: started.elapsed(),
                        bytes_in: 0,
                        checkpoint: None,
                    })
                }
                Err(error) => {
                    SyncEvent::Fatal(recovery::fatal_for_error(error, RecoveryClass::Fatal))
                }
            }
        })
        .chain(stream::once(async { SyncEvent::Done(None) })),
    )
}

pub(crate) fn scope_lifecycle_stream(
    client: Arc<GmailClient>,
    cache: ScopeCache,
    shutdown: CancellationToken,
) -> AccountStream<ScopeLifecycle> {
    let state = LifecycleState {
        client,
        cache,
        shutdown,
        pending: VecDeque::new(),
        initialized: false,
    };
    Box::pin(stream::unfold(state, |mut state| async move {
        loop {
            if let Some(event) = state.pending.pop_front() {
                return Some((event, state));
            }
            if state.shutdown.is_cancelled() {
                return None;
            }
            if state.initialized {
                tokio::select! {
                    () = state.shutdown.cancelled() => return None,
                    () = tokio::time::sleep(LIFECYCLE_POLL_INTERVAL) => {}
                }
            } else {
                state.initialized = true;
            }

            let old = snapshot(&state.cache);
            match refresh_scope_snapshot(&state.client, &state.cache).await {
                Ok(new) => {
                    state.pending = diff_snapshots(&old, &new).into();
                }
                Err(error) => {
                    tracing::warn!("gmail label lifecycle poll failed: {error}");
                }
            }
        }
    }))
}

pub(crate) async fn labels_for_flags(
    client: &Arc<GmailClient>,
    cache: &ScopeCache,
) -> Vec<GmailLabel> {
    let current = snapshot(cache);
    if !current.is_stale() {
        return current.labels;
    }
    match refresh_scope_snapshot(client, cache).await {
        Ok(snapshot) => snapshot.labels,
        Err(error) => {
            tracing::warn!("gmail label refresh failed during flag translation: {error}");
            current.labels
        }
    }
}

pub(crate) fn snapshot(cache: &ScopeCache) -> ScopeSnapshot {
    cache
        .read()
        .map(|guard| guard.clone())
        .unwrap_or_else(|_| ScopeSnapshot::empty())
}

pub(crate) async fn refresh_scope_snapshot(
    client: &GmailClient,
    cache: &ScopeCache,
) -> crate::Result<ScopeSnapshot> {
    let labels = client.list_labels().await?;
    let snapshot = ScopeSnapshot {
        labels,
        fetched_at: Instant::now(),
    };
    if let Ok(mut guard) = cache.write() {
        *guard = snapshot.clone();
    }
    Ok(snapshot)
}

fn diff_snapshots(old: &ScopeSnapshot, new: &ScopeSnapshot) -> Vec<ScopeLifecycle> {
    let old_by_id = old
        .labels
        .iter()
        .map(|label| (label.id.as_str(), label.name.as_str()))
        .collect::<HashMap<_, _>>();
    let new_by_id = new
        .labels
        .iter()
        .map(|label| (label.id.as_str(), label.name.as_str()))
        .collect::<HashMap<_, _>>();

    let mut events = Vec::new();
    for label in &new.labels {
        if !old_by_id.contains_key(label.id.as_str()) {
            events.push(ScopeLifecycle::Created(MembershipScope::Label(LabelId(
                label.id.clone(),
            ))));
        } else if old_by_id.get(label.id.as_str()).is_some_and(|old_name| {
            *old_name != label.name.as_str()
                && label
                    .label_type
                    .as_deref()
                    .is_none_or(|label_type| label_type != "system")
        }) {
            events.push(ScopeLifecycle::Renamed {
                old: MembershipScope::Label(LabelId(label.id.clone())),
                new: MembershipScope::Label(LabelId(label.id.clone())),
            });
        }
    }
    for label in &old.labels {
        if !new_by_id.contains_key(label.id.as_str()) {
            events.push(ScopeLifecycle::Deleted(MembershipScope::Label(LabelId(
                label.id.clone(),
            ))));
        }
    }
    events
}
