//! Gmail history-id driven change stream.
//!
//! All error paths route through `account_error::into_account_error`. The
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
use super::error as account_error;

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
                let account_error = account_error::into_account_error(
                    Error::Local(GmailLocalError::Internal {
                        detail: "gmail change stream missing cursor".to_string(),
                    }),
                    account_error::GmailErrorContext::changes(),
                );
                return Some((SyncEvent::Terminated(account_error), state));
            };
            let decoded = match decode_gmail_state(&cursor.server_state) {
                Ok(decoded) => decoded,
                Err(error) => {
                    state.finished = true;
                    state.emitted_done = true;
                    let account_error = account_error::into_account_error(
                        error,
                        account_error::GmailErrorContext::changes(),
                    );
                    return Some((SyncEvent::Terminated(account_error), state));
                }
            };
            if decoded.profile_email != state.profile.email_address {
                state.finished = true;
                state.emitted_done = true;
                let account_error = account_error::into_account_error(
                    Error::Local(GmailLocalError::AccountIdentityMismatch {
                        cursor_email: decoded.profile_email,
                        profile_email: state.profile.email_address.clone(),
                    }),
                    account_error::GmailErrorContext::changes(),
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
                    let account_error = account_error::into_account_error(
                        Error::Local(GmailLocalError::AccountIdentityMismatch {
                            cursor_email: decoded.profile_email,
                            profile_email: current.email_address,
                        }),
                        account_error::GmailErrorContext::changes(),
                    );
                    return Some((SyncEvent::Terminated(account_error), state));
                }
                Err(error) => {
                    state.finished = true;
                    state.emitted_done = true;
                    let account_error = account_error::into_account_error(
                        error,
                        account_error::GmailErrorContext::changes(),
                    );
                    return Some((SyncEvent::Terminated(account_error), state));
                }
            }
        }

        let Some(start_history_id) = state.start_history_id.as_deref() else {
            state.finished = true;
            state.emitted_done = true;
            let account_error = account_error::into_account_error(
                Error::missing_field("start_history_id", "gmail change stream"),
                account_error::GmailErrorContext::changes(),
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
                        let account_error = account_error::into_account_error(
                            Error::missing_field(
                                "historyId",
                                format!("gmail history response invalid: {error}"),
                            ),
                            account_error::GmailErrorContext::changes(),
                        );
                        return Some((SyncEvent::Terminated(account_error), state));
                    }
                };
                let items = changes_from_history(&response.history);
                let is_final = response.next_page_token.is_none();
                let checkpoint =
                    checkpoint_for_history_page(is_final, history_id, &state.profile.email_address);
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
                        checkpoint,
                    }),
                    state,
                ))
            }
            Err(error) => {
                state.finished = true;
                state.emitted_done = true;
                let account_error = account_error::into_account_error(
                    error,
                    account_error::GmailErrorContext::changes(),
                );
                Some((SyncEvent::Terminated(account_error), state))
            }
        }
    }))
}

/// Only the final page of a history walk may advance the durable cursor.
///
/// `response.history_id` is the mailbox's *current* history record, so it
/// is the same value on every page of the walk rather than a per-page
/// resume marker, and `GmailChangeState` carries no page token to record
/// how far into the walk we are. Checkpointing an intermediate page would
/// therefore tell the engine "durably at 400" while pages two and three
/// are still unread; a restart or a `pause()` landing on that batch
/// resumes from 400 and those changes are gone for good, because Gmail
/// cannot re-serve them.
///
/// If mid-walk resumability is ever wanted, it needs a schema bump:
/// `GMAIL_SCHEMA_VERSION` to 2, a `page_token` on `GmailChangeState`, and
/// `ChangeCursor::advanced_through` populated from it - the shape
/// `crates/graph` already uses.
fn checkpoint_for_history_page(
    is_final: bool,
    history_id: u64,
    profile_email: &str,
) -> Option<Checkpoint> {
    is_final.then(|| Checkpoint::Change(cursor_for_history(history_id, profile_email)))
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
    message.label_ids.clone()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn only_the_final_history_page_advances_the_durable_cursor() {
        assert!(
            checkpoint_for_history_page(false, 400, "person@example.com").is_none(),
            "Gmail page tokens are not represented in the cursor, so an intermediate page cannot be resumed",
        );
        assert!(matches!(
            checkpoint_for_history_page(true, 400, "person@example.com"),
            Some(Checkpoint::Change(_))
        ));
    }

    fn history(value: serde_json::Value) -> Vec<GmailHistoryItem> {
        serde_json::from_value(value).expect("history fixture deserializes")
    }

    fn message(id: &str, labels: &[&str]) -> serde_json::Value {
        json!({
            "id": id,
            "threadId": format!("t-{id}"),
            "labelIds": labels,
        })
    }

    fn object_changes(changes: &[Change]) -> Vec<(String, ObjectChangeKind)> {
        changes
            .iter()
            .filter_map(|change| match change {
                Change::ObjectChange(oc) => Some((oc.id.0.clone(), oc.kind)),
                _ => None,
            })
            .collect()
    }

    fn scope_changes(changes: &[Change]) -> Vec<(String, String, ScopeChangeKind)> {
        changes
            .iter()
            .filter_map(|change| match change {
                Change::ScopeChange(sc) => match &sc.membership {
                    MembershipScope::Label(LabelId(label)) => {
                        Some((sc.id.0.clone(), label.clone(), sc.kind))
                    }
                    _ => None,
                },
                _ => None,
            })
            .collect()
    }

    #[test]
    fn empty_history_yields_no_changes() {
        assert!(changes_from_history(&[]).is_empty());
        assert!(changes_from_history(&history(json!([{}]))).is_empty());
    }

    /// A `messagesAdded` record is both an object birth and a
    /// membership announcement: the engine needs the `Created` row to
    /// materialise the object and one `ScopeChange::Added` per label so
    /// the object lands in the right containers without a follow-up
    /// hydrate.
    #[test]
    fn messages_added_emits_created_then_one_scope_row_per_label() {
        let changes = changes_from_history(&history(json!([{
            "messagesAdded": [{ "message": message("m1", &["INBOX", "UNREAD"]) }],
        }])));

        assert_eq!(
            object_changes(&changes),
            vec![("m1".to_owned(), ObjectChangeKind::Created)]
        );
        assert_eq!(
            scope_changes(&changes),
            vec![
                ("m1".to_owned(), "INBOX".to_owned(), ScopeChangeKind::Added),
                ("m1".to_owned(), "UNREAD".to_owned(), ScopeChangeKind::Added),
            ]
        );
        assert!(
            matches!(changes.first(), Some(Change::ObjectChange(_))),
            "the object must be created before its memberships are announced"
        );
    }

    /// Gmail omits `labelIds` on a message with no labels; the row must
    /// still create the object.
    #[test]
    fn messages_added_without_labels_still_creates_the_object() {
        let changes = changes_from_history(&history(json!([{
            "messagesAdded": [{ "message": { "id": "m2", "threadId": "t2" } }],
        }])));
        assert_eq!(
            object_changes(&changes),
            vec![("m2".to_owned(), ObjectChangeKind::Created)]
        );
        assert!(scope_changes(&changes).is_empty());
    }

    #[test]
    fn messages_deleted_emits_a_destroyed_row_only() {
        let changes = changes_from_history(&history(json!([{
            "messagesDeleted": [{ "message": message("m3", &["INBOX"]) }],
        }])));
        assert_eq!(
            object_changes(&changes),
            vec![("m3".to_owned(), ObjectChangeKind::Destroyed)]
        );
        assert!(
            scope_changes(&changes).is_empty(),
            "a destroyed message needs no membership teardown row"
        );
    }

    #[test]
    fn label_add_and_remove_become_scoped_membership_rows() {
        let changes = changes_from_history(&history(json!([{
            "labelsAdded": [{
                "message": message("m4", &["INBOX"]),
                "labelIds": ["Label_1", "STARRED"],
            }],
            "labelsRemoved": [{
                "message": message("m4", &["INBOX"]),
                "labelIds": ["UNREAD"],
            }],
        }])));

        assert!(
            object_changes(&changes).is_empty(),
            "a label flip is not an object birth or death"
        );
        assert_eq!(
            scope_changes(&changes),
            vec![
                (
                    "m4".to_owned(),
                    "Label_1".to_owned(),
                    ScopeChangeKind::Added
                ),
                (
                    "m4".to_owned(),
                    "STARRED".to_owned(),
                    ScopeChangeKind::Added
                ),
                (
                    "m4".to_owned(),
                    "UNREAD".to_owned(),
                    ScopeChangeKind::Removed
                ),
            ]
        );
    }

    /// The label rows carry the *changed* labels from `labelIds`, not
    /// the message's full label set. Pinning this matters because the
    /// wrapper also carries a `message` whose own `labelIds` is the
    /// post-change full set - reading the wrong one would announce
    /// memberships that did not change.
    #[test]
    fn label_rows_use_the_delta_not_the_messages_full_label_set() {
        let changes = changes_from_history(&history(json!([{
            "labelsAdded": [{
                "message": message("m5", &["INBOX", "UNREAD", "Label_9"]),
                "labelIds": ["Label_9"],
            }],
        }])));
        assert_eq!(
            scope_changes(&changes),
            vec![(
                "m5".to_owned(),
                "Label_9".to_owned(),
                ScopeChangeKind::Added
            )]
        );
    }

    /// Multiple history records fold into one flat, order-preserving
    /// change list; the engine relies on history order for correctness
    /// when an object is created and then relabelled in the same page.
    #[test]
    fn multiple_history_records_preserve_wire_order() {
        let changes = changes_from_history(&history(json!([
            { "messagesAdded": [{ "message": message("m6", &["INBOX"]) }] },
            {
                "labelsRemoved": [{
                    "message": message("m6", &["INBOX"]),
                    "labelIds": ["INBOX"],
                }],
            },
            { "messagesDeleted": [{ "message": message("m6", &[]) }] },
        ])));

        assert_eq!(
            object_changes(&changes),
            vec![
                ("m6".to_owned(), ObjectChangeKind::Created),
                ("m6".to_owned(), ObjectChangeKind::Destroyed),
            ]
        );
        assert_eq!(
            scope_changes(&changes),
            vec![
                ("m6".to_owned(), "INBOX".to_owned(), ScopeChangeKind::Added),
                (
                    "m6".to_owned(),
                    "INBOX".to_owned(),
                    ScopeChangeKind::Removed
                ),
            ]
        );
    }

    /// One history record can carry every category at once; all four
    /// buckets must be drained in the fixed added / deleted / labelled
    /// order the mapper documents.
    #[test]
    fn a_single_record_drains_all_four_buckets() {
        let changes = changes_from_history(&history(json!([{
            "messagesAdded": [{ "message": message("a", &[]) }],
            "messagesDeleted": [{ "message": message("b", &[]) }],
            "labelsAdded": [{ "message": message("c", &[]), "labelIds": ["L"] }],
            "labelsRemoved": [{ "message": message("d", &[]), "labelIds": ["L"] }],
        }])));
        assert_eq!(changes.len(), 4);
        assert_eq!(
            object_changes(&changes),
            vec![
                ("a".to_owned(), ObjectChangeKind::Created),
                ("b".to_owned(), ObjectChangeKind::Destroyed),
            ]
        );
    }
}
