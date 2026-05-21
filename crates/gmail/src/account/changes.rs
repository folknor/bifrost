use std::sync::Arc;
use std::time::Instant;

use bifrost_types::{
    AccountStream, Batch, Change, Checkpoint, LabelId, MembershipScope, ObjectChange,
    ObjectChangeKind, ObjectId, PageBoundary, RecoveryClass, ScopeChange, ScopeChangeKind,
    SyncEvent,
};
use futures::stream;

use crate::client::GmailClient;
use crate::types::{GmailHistoryItem, GmailMessage, GmailProfile};

use super::cursor::{cursor_for_history, decode_gmail_state};
use super::recovery;

pub(crate) fn changes_stream(
    client: Arc<GmailClient>,
    profile: GmailProfile,
    cursor: bifrost_types::ChangeCursor,
) -> AccountStream<SyncEvent<Change>> {
    let state = ChangeState {
        client,
        profile,
        cursor: Some(cursor),
        page_token: None,
        start_history_id: None,
        finished: false,
        emitted_done: false,
        checked_profile: false,
    };

    Box::pin(stream::unfold(state, |mut state| async move {
        if state.finished {
            if state.emitted_done {
                return None;
            }
            state.emitted_done = true;
            return Some((SyncEvent::Done(None), state));
        }

        let started = Instant::now();
        if !state.checked_profile {
            let Some(cursor) = state.cursor.take() else {
                state.finished = true;
                state.emitted_done = true;
                return Some((
                    SyncEvent::Fatal(bifrost_types::Fatal {
                        recovery: RecoveryClass::Fatal,
                        message: "gmail change stream missing cursor".to_string(),
                        source: Some(bifrost_types::Error::Other(
                            "gmail change stream missing cursor".to_string(),
                        )),
                    }),
                    state,
                ));
            };
            let decoded = match decode_gmail_state(&cursor.server_state) {
                Ok(decoded) => decoded,
                Err(error) => {
                    state.finished = true;
                    state.emitted_done = true;
                    return Some((
                        SyncEvent::Fatal(recovery::fatal_for_account_error(
                            error,
                            RecoveryClass::SchemaIncompatible,
                        )),
                        state,
                    ));
                }
            };
            if decoded.profile_email != state.profile.email_address {
                state.finished = true;
                state.emitted_done = true;
                return Some((
                    SyncEvent::Fatal(bifrost_types::Fatal {
                        recovery: RecoveryClass::Fatal,
                        message: format!(
                            "gmail cursor belongs to {}, opened account is {}",
                            decoded.profile_email, state.profile.email_address
                        ),
                        source: Some(bifrost_types::Error::Other(
                            "gmail account identity changed".to_string(),
                        )),
                    }),
                    state,
                ));
            }
            match state.client.get_profile().await {
                Ok(current) if current.email_address == decoded.profile_email => {
                    state.start_history_id = Some(decoded.history_id.to_string());
                    state.checked_profile = true;
                }
                Ok(current) => {
                    state.finished = true;
                    state.emitted_done = true;
                    return Some((
                        SyncEvent::Fatal(bifrost_types::Fatal {
                            recovery: RecoveryClass::Fatal,
                            message: format!(
                                "gmail account identity changed from {} to {}",
                                decoded.profile_email, current.email_address
                            ),
                            source: Some(bifrost_types::Error::Other(
                                "gmail account identity changed".to_string(),
                            )),
                        }),
                        state,
                    ));
                }
                Err(error) => {
                    let recovery = recovery::classify_general_error(&error);
                    state.finished = true;
                    state.emitted_done = true;
                    return Some((
                        SyncEvent::Fatal(recovery::fatal_for_error(error, recovery)),
                        state,
                    ));
                }
            }
        }

        let Some(start_history_id) = state.start_history_id.as_deref() else {
            state.finished = true;
            state.emitted_done = true;
            return Some((
                SyncEvent::Fatal(bifrost_types::Fatal {
                    recovery: RecoveryClass::Fatal,
                    message: "gmail change stream has no start history id".to_string(),
                    source: Some(bifrost_types::Error::Other(
                        "gmail change stream has no start history id".to_string(),
                    )),
                }),
                state,
            ));
        };

        match state
            .client
            .get_history(start_history_id, state.page_token.as_deref())
            .await
        {
            Ok(response) => {
                let history_id = match response.history_id.parse::<u64>() {
                    Ok(history_id) => history_id,
                    Err(error) => {
                        state.finished = true;
                        state.emitted_done = true;
                        return Some((
                            SyncEvent::Fatal(bifrost_types::Fatal {
                                recovery: RecoveryClass::Fatal,
                                message: format!(
                                    "gmail history response carried invalid history id: {error}"
                                ),
                                source: Some(bifrost_types::Error::Other(error.to_string())),
                            }),
                            state,
                        ));
                    }
                };
                let checkpoint = Checkpoint::Change(cursor_for_history(
                    history_id,
                    &state.profile.email_address,
                ));
                let items = changes_from_history(&response.history);
                let is_final = response.next_page_token.is_none();
                state.page_token = response.next_page_token;
                if is_final {
                    state.finished = true;
                }
                Some((
                    SyncEvent::Batch(Batch {
                        items,
                        page_boundary: if is_final {
                            PageBoundary::Final
                        } else {
                            PageBoundary::Page
                        },
                        server_latency: started.elapsed(),
                        bytes_in: 0,
                        checkpoint: Some(checkpoint),
                    }),
                    state,
                ))
            }
            Err(error) => {
                let recovery = recovery::classify_history_error(&error);
                state.finished = true;
                state.emitted_done = true;
                Some((
                    SyncEvent::Fatal(recovery::fatal_for_error(error, recovery)),
                    state,
                ))
            }
        }
    }))
}

struct ChangeState {
    client: Arc<GmailClient>,
    profile: GmailProfile,
    cursor: Option<bifrost_types::ChangeCursor>,
    page_token: Option<String>,
    start_history_id: Option<String>,
    finished: bool,
    emitted_done: bool,
    checked_profile: bool,
}

fn changes_from_history(history: &[GmailHistoryItem]) -> Vec<Change> {
    let mut changes = Vec::new();
    for item in history {
        for added in &item.messages_added {
            push_message_added(&mut changes, &added.message);
        }
        for deleted in &item.messages_deleted {
            changes.push(Change::ObjectChange(ObjectChange {
                id: ObjectId(deleted.message.id.clone()),
                kind: ObjectChangeKind::Destroyed,
            }));
        }
        for added in &item.labels_added {
            for label_id in &added.label_ids {
                changes.push(Change::ScopeChange(ScopeChange {
                    id: ObjectId(added.message.id.clone()),
                    membership: MembershipScope::Label(LabelId(label_id.clone())),
                    kind: ScopeChangeKind::Added,
                }));
            }
        }
        for removed in &item.labels_removed {
            for label_id in &removed.label_ids {
                changes.push(Change::ScopeChange(ScopeChange {
                    id: ObjectId(removed.message.id.clone()),
                    membership: MembershipScope::Label(LabelId(label_id.clone())),
                    kind: ScopeChangeKind::Removed,
                }));
            }
        }
    }
    changes
}

fn push_message_added(changes: &mut Vec<Change>, message: &GmailMessage) {
    changes.push(Change::ObjectChange(ObjectChange {
        id: ObjectId(message.id.clone()),
        kind: ObjectChangeKind::Created,
    }));
    for label_id in &message.label_ids {
        changes.push(Change::ScopeChange(ScopeChange {
            id: ObjectId(message.id.clone()),
            membership: MembershipScope::Label(LabelId(label_id.clone())),
            kind: ScopeChangeKind::Added,
        }));
    }
}
