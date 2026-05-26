//! Gmail history-id driven change stream.
//!
//! All error paths route through `recovery::into_account_error`. The
//! history endpoint context maps 404 / 410 / `historyNotFound` /
//! `failedPrecondition` to `SyncState(CursorInvalid)` with the
//! account cursor scope, which the central recovery mapper resolves
//! to `Engine(RestartScope(CursorScope::Account))`.

use std::sync::Arc;
use std::time::Instant;

use bifrost_types::{
    AccountOperation, AccountStream, Batch, Change, Checkpoint, LabelId, MembershipScope,
    ObjectChange, ObjectChangeKind, ObjectId, PageBoundary, ScopeChange, ScopeChangeKind,
    SyncEvent,
};
use futures::stream;

use crate::client::GmailClient;
use crate::error::{Error, GmailLocalError};
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
                let account_error = recovery::into_account_error(
                    Error::Local(GmailLocalError::Internal {
                        detail: "gmail change stream missing cursor".to_string(),
                    }),
                    recovery::GmailErrorContext::changes(),
                );
                return Some((SyncEvent::Terminated(account_error), state));
            };
            let decoded = match decode_gmail_state(&cursor.server_state) {
                Ok(decoded) => decoded,
                Err(error) => {
                    state.finished = true;
                    state.emitted_done = true;
                    let account_error =
                        recovery::into_account_error(error, recovery::GmailErrorContext::changes());
                    return Some((SyncEvent::Terminated(account_error), state));
                }
            };
            if decoded.profile_email != state.profile.email_address {
                state.finished = true;
                state.emitted_done = true;
                let account_error = recovery::into_account_error(
                    Error::Local(GmailLocalError::AccountIdentityMismatch {
                        cursor_email: decoded.profile_email,
                        profile_email: state.profile.email_address.clone(),
                    }),
                    recovery::GmailErrorContext::changes(),
                );
                return Some((SyncEvent::Terminated(account_error), state));
            }
            match state.client.get_profile().await {
                Ok(current) if current.email_address == decoded.profile_email => {
                    state.start_history_id = Some(decoded.history_id.to_string());
                    state.checked_profile = true;
                }
                Ok(current) => {
                    state.finished = true;
                    state.emitted_done = true;
                    let account_error = recovery::into_account_error(
                        Error::Local(GmailLocalError::AccountIdentityMismatch {
                            cursor_email: decoded.profile_email,
                            profile_email: current.email_address,
                        }),
                        recovery::GmailErrorContext::changes(),
                    );
                    return Some((SyncEvent::Terminated(account_error), state));
                }
                Err(error) => {
                    state.finished = true;
                    state.emitted_done = true;
                    let account_error =
                        recovery::into_account_error(error, recovery::GmailErrorContext::changes());
                    return Some((SyncEvent::Terminated(account_error), state));
                }
            }
        }

        let Some(start_history_id) = state.start_history_id.as_deref() else {
            state.finished = true;
            state.emitted_done = true;
            let account_error = recovery::into_account_error(
                Error::missing_field("start_history_id", "gmail change stream"),
                recovery::GmailErrorContext::changes(),
            );
            return Some((SyncEvent::Terminated(account_error), state));
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
                        let account_error = recovery::into_account_error(
                            Error::missing_field(
                                "historyId",
                                format!("gmail history response invalid: {error}"),
                            ),
                            recovery::GmailErrorContext::changes(),
                        );
                        return Some((SyncEvent::Terminated(account_error), state));
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
                state.finished = true;
                state.emitted_done = true;
                let account_error =
                    recovery::into_account_error(error, recovery::GmailErrorContext::changes());
                Some((SyncEvent::Terminated(account_error), state))
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
            let object_id = ObjectId(added.message.id.clone());
            changes.push(Change::ObjectChange(ObjectChange {
                id: object_id.clone(),
                kind: ObjectChangeKind::Created,
            }));
            for label in label_ids(&added.message) {
                changes.push(Change::ScopeChange(ScopeChange {
                    id: object_id.clone(),
                    membership: MembershipScope::Label(LabelId(label.clone())),
                    kind: ScopeChangeKind::Added,
                }));
            }
        }
        for deleted in &item.messages_deleted {
            changes.push(Change::ObjectChange(ObjectChange {
                id: ObjectId(deleted.message.id.clone()),
                kind: ObjectChangeKind::Destroyed,
            }));
        }
        for labels_added in &item.labels_added {
            let object_id = ObjectId(labels_added.message.id.clone());
            for label in &labels_added.label_ids {
                changes.push(Change::ScopeChange(ScopeChange {
                    id: object_id.clone(),
                    membership: MembershipScope::Label(LabelId(label.clone())),
                    kind: ScopeChangeKind::Added,
                }));
            }
        }
        for labels_removed in &item.labels_removed {
            let object_id = ObjectId(labels_removed.message.id.clone());
            for label in &labels_removed.label_ids {
                changes.push(Change::ScopeChange(ScopeChange {
                    id: object_id.clone(),
                    membership: MembershipScope::Label(LabelId(label.clone())),
                    kind: ScopeChangeKind::Removed,
                }));
            }
        }
    }
    // AccountOperation::SyncChanges is implicit in the changes context.
    let _ = AccountOperation::SyncChanges;
    changes
}

fn label_ids(message: &GmailMessage) -> Vec<String> {
    message.label_ids.iter().cloned().collect()
}
