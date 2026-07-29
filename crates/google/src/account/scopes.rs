use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use bifrost_types::{
    AccountStream, Batch, CursorScope, LabelId, MembershipScope, PageBoundary, ScopeLifecycle,
    ScopeLifecycleEvent, SyncEvent,
};
use futures::{StreamExt, stream};
use tokio_util::sync::CancellationToken;

use crate::client::GmailClient;
use crate::types::GmailLabel;

use super::error;

const LIFECYCLE_POLL_INTERVAL: Duration = Duration::from_secs(30);
pub(crate) const SCOPE_CACHE_STALE_AFTER: Duration = Duration::from_secs(300);

#[derive(Debug, Clone)]
pub(crate) struct ScopeSnapshot {
    pub(crate) labels: Vec<GmailLabel>,
    /// When `list_labels` last succeeded, or `None` for a cache that has
    /// never been populated.
    ///
    /// This is an `Option` rather than a bare `Instant` so that the
    /// never-populated state is representable. Stamping `Instant::now()`
    /// on an empty snapshot made `is_stale()` report FRESH for the first
    /// five minutes after `open()`, so the first flag mutation of a
    /// session translated against an empty vocabulary and silently
    /// no-opped. Keying staleness on `labels.is_empty()` instead would
    /// fix that but re-fetch forever for any account whose label list is
    /// legitimately empty; "was it ever fetched" is the actual question.
    pub(crate) fetched_at: Option<Instant>,
}

impl ScopeSnapshot {
    pub(crate) fn empty() -> Self {
        Self {
            labels: Vec::new(),
            fetched_at: None,
        }
    }

    pub(crate) fn is_stale(&self) -> bool {
        self.fetched_at
            .is_none_or(|fetched_at| fetched_at.elapsed() > SCOPE_CACHE_STALE_AFTER)
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
                    // Success: emit the batch then Done.
                    let batch = SyncEvent::Batch(Batch {
                        items,
                        page_boundary: PageBoundary::Final,
                        server_latency: started.elapsed(),
                        bytes_in: 0,
                        checkpoint: None,
                    });
                    vec![batch, SyncEvent::Done(None)]
                }
                Err(error) => {
                    let account_error = error::into_account_error(
                        error,
                        error::GmailErrorContext::containers_list(),
                    );
                    vec![SyncEvent::Terminated(account_error)]
                }
            }
        })
        .flat_map(stream::iter),
    )
}

pub(crate) fn scope_lifecycle_stream(
    client: Arc<GmailClient>,
    cache: ScopeCache,
    shutdown: CancellationToken,
) -> AccountStream<ScopeLifecycleEvent> {
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
                return Some((ScopeLifecycleEvent::Lifecycle(event), state));
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
                    state.pending = lifecycle_diff(&old, &new).into();
                }
                Err(error) => {
                    // Classify: terminal / engine-action -> emit
                    // Terminated and end the stream so the engine
                    // escalates. Retry classes continue with the
                    // backoff so transient hiccups don't pollute the
                    // engine surface.
                    let acct = super::error::into_account_error(
                        error,
                        super::error::GmailErrorContext::containers_list(),
                    );
                    if acct.recovery().is_terminal() || acct.recovery().requires_engine_action() {
                        return Some((ScopeLifecycleEvent::Terminated(acct), state));
                    }
                    tracing::warn!(
                        target: "bifrost.gmail.scope_lifecycle",
                        kind = ?acct.kind(),
                        message_key = acct.message_key(),
                        "gmail label lifecycle poll: transient failure"
                    );
                }
            }
        }
    }))
}

fn lifecycle_diff(old: &ScopeSnapshot, new: &ScopeSnapshot) -> Vec<ScopeLifecycle> {
    if old.fetched_at.is_none() {
        Vec::new()
    } else {
        diff_snapshots(old, new)
    }
}

/// The label vocabulary every flag-canonicalizing call site must go
/// through.
///
/// Never reach for `snapshot(cache)` directly for this: the cache starts
/// empty, and canonicalizing against an empty vocabulary is silently wrong
/// in both directions. Reading, `canonical_flags` falls back to the id in
/// the name slot and destabilises `Fingerprint.flags_hash`; writing, every
/// `$gmail-label:` flag becomes unsupported and the batch reports
/// `Skipped`. A refresh failure with nothing cached therefore propagates
/// as an error - a stale-but-populated vocabulary is degraded, an empty
/// one is unusable.
pub(crate) async fn labels_for_flags(
    client: &Arc<GmailClient>,
    cache: &ScopeCache,
) -> crate::Result<Vec<GmailLabel>> {
    let current = snapshot(cache);
    if !current.is_stale() {
        return Ok(current.labels);
    }
    match refresh_scope_snapshot(client, cache).await {
        Ok(snapshot) => Ok(snapshot.labels),
        Err(error) if current.fetched_at.is_some() => {
            tracing::warn!("gmail label refresh failed during flag translation: {error}");
            Ok(current.labels)
        }
        Err(error) => Err(error),
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
        fetched_at: Some(Instant::now()),
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

#[cfg(test)]
mod tests {
    use super::*;

    fn label(id: &str, name: &str, label_type: &str) -> GmailLabel {
        GmailLabel {
            id: id.to_owned(),
            name: name.to_owned(),
            label_type: Some(label_type.to_owned()),
            color: None,
        }
    }

    #[test]
    fn first_successful_lifecycle_snapshot_seeds_without_created_events() {
        let old = ScopeSnapshot::empty();
        let new = ScopeSnapshot {
            labels: vec![label("Label_1", "One", "user")],
            fetched_at: Some(Instant::now()),
        };
        assert!(lifecycle_diff(&old, &new).is_empty());
    }

    #[test]
    fn populated_lifecycle_snapshot_still_emits_real_changes() {
        let old = ScopeSnapshot {
            labels: vec![label("Label_1", "One", "user")],
            fetched_at: Some(Instant::now()),
        };
        let new = ScopeSnapshot {
            labels: vec![
                label("Label_1", "One", "user"),
                label("Label_2", "Two", "user"),
            ],
            fetched_at: Some(Instant::now()),
        };
        assert!(matches!(
            lifecycle_diff(&old, &new).as_slice(),
            [ScopeLifecycle::Created(MembershipScope::Label(LabelId(id)))]
                if id == "Label_2"
        ));
    }

    fn snapshot_of(labels: Vec<GmailLabel>) -> ScopeSnapshot {
        ScopeSnapshot {
            labels,
            fetched_at: Some(Instant::now()),
        }
    }

    fn label_id(scope: &MembershipScope) -> &str {
        match scope {
            MembershipScope::Label(LabelId(id)) => id.as_str(),
            _ => panic!("gmail lifecycle events are always label-scoped"),
        }
    }

    #[test]
    fn identical_snapshots_produce_no_events() {
        let snap = snapshot_of(vec![label("Label_1", "Work", "user")]);
        let again = snapshot_of(vec![label("Label_1", "Work", "user")]);
        assert!(diff_snapshots(&snap, &again).is_empty());
    }

    #[test]
    fn a_new_label_is_created_and_a_vanished_one_is_deleted() {
        let old = snapshot_of(vec![label("Label_1", "Work", "user")]);
        let new = snapshot_of(vec![label("Label_2", "Home", "user")]);
        let events = diff_snapshots(&old, &new);
        assert_eq!(events.len(), 2);
        match &events[0] {
            ScopeLifecycle::Created(scope) => assert_eq!(label_id(scope), "Label_2"),
            other => panic!("expected Created first, got {other:?}"),
        }
        match &events[1] {
            ScopeLifecycle::Deleted(scope) => assert_eq!(label_id(scope), "Label_1"),
            other => panic!("expected Deleted second, got {other:?}"),
        }
    }

    /// Gmail keeps the label id across a rename, so a name change on a
    /// user label surfaces as `Renamed`.
    #[test]
    fn a_user_label_name_change_is_a_rename() {
        let old = snapshot_of(vec![label("Label_1", "Work", "user")]);
        let new = snapshot_of(vec![label("Label_1", "Work Stuff", "user")]);
        let events = diff_snapshots(&old, &new);
        assert_eq!(events.len(), 1);
        assert!(matches!(events[0], ScopeLifecycle::Renamed { .. }));
    }

    /// Gmail's stable label id makes Renamed an invalidation signal.
    /// The consumer re-reads container metadata to obtain the new name.
    #[test]
    fn rename_events_carry_the_same_scope_on_both_sides() {
        let old = snapshot_of(vec![label("Label_1", "Work", "user")]);
        let new = snapshot_of(vec![label("Label_1", "Renamed", "user")]);
        match &diff_snapshots(&old, &new)[0] {
            ScopeLifecycle::Renamed { old, new } => {
                assert_eq!(label_id(old), label_id(new));
                assert_eq!(label_id(old), "Label_1");
            }
            other => panic!("expected Renamed, got {other:?}"),
        }
    }

    /// System labels are localised by Gmail (the `INBOX` label's `name`
    /// follows the account language), so a locale flip must not be
    /// reported as a user-visible rename.
    #[test]
    fn system_label_name_changes_are_not_renames() {
        let old = snapshot_of(vec![label("INBOX", "INBOX", "system")]);
        let new = snapshot_of(vec![label("INBOX", "Innboks", "system")]);
        assert!(diff_snapshots(&old, &new).is_empty());
    }

    /// A label whose `type` Gmail omitted is treated as user-shaped, so
    /// its rename still surfaces. The alternative (swallowing it) would
    /// silently drop renames on any future label shape.
    #[test]
    fn a_label_with_no_type_is_treated_as_renameable() {
        let old = snapshot_of(vec![GmailLabel {
            id: "Label_3".to_owned(),
            name: "Old".to_owned(),
            label_type: None,
            color: None,
        }]);
        let new = snapshot_of(vec![GmailLabel {
            id: "Label_3".to_owned(),
            name: "New".to_owned(),
            label_type: None,
            color: None,
        }]);
        assert_eq!(diff_snapshots(&old, &new).len(), 1);
    }

    /// A color-only edit is not a lifecycle event; only presence and
    /// name participate in the diff.
    #[test]
    fn a_color_only_change_is_not_a_lifecycle_event() {
        let old = snapshot_of(vec![label("Label_1", "Work", "user")]);
        let mut recolored = label("Label_1", "Work", "user");
        recolored.color = Some(crate::types::GmailLabelColor {
            background_color: Some("#000000".to_owned()),
            text_color: Some("#ffffff".to_owned()),
        });
        assert!(diff_snapshots(&old, &snapshot_of(vec![recolored])).is_empty());
    }

    #[test]
    fn diffing_against_an_empty_old_snapshot_creates_everything() {
        let new = snapshot_of(vec![
            label("Label_1", "Work", "user"),
            label("INBOX", "INBOX", "system"),
        ]);
        let events = diff_snapshots(&ScopeSnapshot::empty(), &new);
        assert_eq!(events.len(), 2);
        assert!(
            events
                .iter()
                .all(|event| matches!(event, ScopeLifecycle::Created(_))),
        );
    }

    #[test]
    fn a_never_populated_scope_cache_is_born_stale() {
        let empty = ScopeSnapshot::empty();
        assert!(empty.labels.is_empty());
        assert!(
            empty.is_stale(),
            "the first label-dependent operation must populate the cache",
        );
    }

    #[test]
    fn a_snapshot_older_than_the_stale_window_is_stale() {
        // `Instant` is monotonic-since-boot, so back-dating can fail on
        // a machine that just booted. Skip rather than panic there.
        if let Some(back_dated) =
            Instant::now().checked_sub(SCOPE_CACHE_STALE_AFTER + Duration::from_secs(1))
        {
            let stale = ScopeSnapshot {
                labels: vec![label("Label_1", "Work", "user")],
                fetched_at: Some(back_dated),
            };
            assert!(stale.is_stale());
        }

        let fresh = ScopeSnapshot {
            labels: vec![label("Label_1", "Work", "user")],
            fetched_at: Some(Instant::now()),
        };
        assert!(!fresh.is_stale());
    }

    /// An account whose `labels.list` legitimately returns nothing must
    /// still cache that answer. Keying staleness on `labels.is_empty()`
    /// would make every flag-translating call re-fetch forever.
    #[test]
    fn a_successfully_fetched_empty_label_list_is_fresh() {
        let fetched_empty = ScopeSnapshot {
            labels: Vec::new(),
            fetched_at: Some(Instant::now()),
        };
        assert!(!fetched_empty.is_stale());
    }

    #[test]
    fn snapshot_reads_the_cache_under_the_lock() {
        let cache: ScopeCache = Arc::new(RwLock::new(snapshot_of(vec![label(
            "Label_1", "Work", "user",
        )])));
        assert_eq!(snapshot(&cache).labels.len(), 1);
    }
}
