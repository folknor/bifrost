//! Gmail history-id driven change stream.
//!
//! All error paths route through `account_error::into_account_error`. The
//! history endpoint context maps 404 / 410 / `historyNotFound` /
//! `failedPrecondition` to `SyncState(CursorInvalid)` with the
//! account cursor scope, which the central recovery mapper resolves
//! to `Engine(RestartScope(CursorScope::Account))`.

use std::collections::HashSet;
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

/// Upper bound on `users.history.list` pages in a single walk.
///
/// The walk had no bound at all: it followed `nextPageToken` until the
/// field was absent, so a server that keeps handing one back - a bug, a
/// misbehaving proxy, a token that echoes itself - spins forever issuing
/// quota-bearing requests. The budget is deliberately far above any real
/// mailbox (a history page carries up to 500 records, so this covers five
/// million change records in one walk) because it is a refusal boundary,
/// not a paging policy: it must never fire for a real account.
///
/// Refusal must TERMINATE, never truncate. `changes_stream` checkpoints
/// only on its final page, so stopping early and reporting a normal
/// completion would tell the engine the walk covered ground it never read;
/// a `SyncEvent::Terminated` costs a redone walk and claims nothing.
const MAX_HISTORY_PAGES: usize = 10_000;

pub(crate) fn changes_stream(
    client: Arc<GmailClient>,
    profile: GmailProfile,
    cursor: bifrost_types::ChangeCursor,
) -> AccountStream<SyncEvent<Change>> {
    // One accumulator for the life of this stream. Every request the
    // walk makes - the identity probe as well as each history page -
    // reports into it, and each emitted batch takes and clears it, so
    // consecutive batches partition the traffic instead of each
    // restating a running total.
    let (client, tally) = client.metered();
    let client = Arc::new(client);
    let state = ChangeState {
        client,
        tally,
        profile,
        cursor: Some(cursor),
        page_token: None,
        start_history_id: None,
        walk_history_id: None,
        seen_page_tokens: HashSet::new(),
        pages_walked: 0,
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
            if !decoded
                .profile_email
                .eq_ignore_ascii_case(&state.profile.email_address)
            {
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
                Ok(current)
                    if current
                        .email_address
                        .eq_ignore_ascii_case(&decoded.profile_email) =>
                {
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
                let bytes_in = state.tally.take();
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
                state.pages_walked += 1;
                if let Some(refusal) = walk_refusal(
                    &mut state.seen_page_tokens,
                    state.pages_walked,
                    response.next_page_token.as_deref(),
                ) {
                    // The page just read is discarded on purpose. The walk
                    // has emitted no checkpoint yet (only a final page
                    // does), so the next walk restarts from the same
                    // `startHistoryId` and re-reads it; keeping the page
                    // would buy nothing and risk reading a partial walk as
                    // progress.
                    state.finished = true;
                    state.emitted_done = true;
                    let account_error = account_error::into_account_error(
                        Error::Local(GmailLocalError::Internal { detail: refusal }),
                        account_error::GmailErrorContext::changes(),
                    );
                    return Some((SyncEvent::Terminated(account_error), state));
                }
                let items = changes_from_history(&response.history);
                let is_final = response.next_page_token.is_none();
                let walk_history_id = *state.walk_history_id.get_or_insert(history_id);
                let checkpoint = checkpoint_for_history_page(
                    is_final,
                    walk_history_id,
                    &state.profile.email_address,
                );
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
                        bytes_in,
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

/// Decides whether the walk must refuse to follow `next_page_token`,
/// returning the diagnostic detail for the terminating error.
///
/// Two independent guards, matching `calendars_list`: a repeated token
/// means the server is cycling and no amount of further paging makes
/// progress, and the page budget catches a server that hands out fresh
/// tokens forever. `None` means the walk may continue (or stop normally,
/// when there is no token at all).
fn walk_refusal(
    seen_page_tokens: &mut HashSet<String>,
    pages_walked: usize,
    next_page_token: Option<&str>,
) -> Option<String> {
    let token = next_page_token?;
    if !seen_page_tokens.insert(token.to_string()) {
        return Some("gmail users.history.list repeated a page token".to_string());
    }
    (pages_walked >= MAX_HISTORY_PAGES)
        .then(|| format!("gmail users.history.list exceeded {MAX_HISTORY_PAGES} pages in one walk"))
}

/// Only the final page of a history walk may advance the durable cursor.
///
/// `response.history_id` is the mailbox's current history record at the
/// time each page is requested, not a per-page resume marker. Records can
/// arrive while a multi-page response snapshot is being drained, so the
/// final checkpoint uses the value observed on the first page. That can
/// replay concurrent records on the next walk, but cannot skip them.
/// `GmailChangeState` carries no page token to record how far into the walk
/// we are. Checkpointing an intermediate page would
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
    tally: crate::client::ByteTally,
    profile: GmailProfile,
    cursor: Option<bifrost_types::ChangeCursor>,
    page_token: Option<String>,
    start_history_id: Option<String>,
    walk_history_id: Option<u64>,
    seen_page_tokens: HashSet<String>,
    pages_walked: usize,
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
    use std::sync::Arc;

    use bifrost_net::test_support::{Canned, ScriptedDispatch};
    use bifrost_net::{NetConfig, RetryPolicy, StaticTokenSource};
    use bytes::Bytes;
    use futures::StreamExt;
    use reqwest::StatusCode;

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

    #[tokio::test]
    async fn multi_page_walk_checkpoints_the_first_pages_history_boundary() {
        let client = scripted_client(vec![
            ok_json(json!({ "emailAddress": "person@example.com", "historyId": "100" })),
            ok_json(json!({
                "historyId": "200",
                "history": [{"messagesAdded": [{"message": message("first", &[])}]}],
                "nextPageToken": "page-2"
            })),
            ok_json(json!({
                "historyId": "250",
                "history": [{"messagesAdded": [{"message": message("second", &[])}]}]
            })),
        ]);
        let profile = GmailProfile {
            email_address: "person@example.com".to_string(),
            history_id: "100".to_string(),
        };

        let events = changes_stream(
            client,
            profile,
            cursor_for_history(100, "person@example.com"),
        )
        .collect::<Vec<_>>()
        .await;

        let SyncEvent::Batch(final_page) = &events[1] else {
            panic!("second history response must be a batch");
        };
        let Some(Checkpoint::Change(cursor)) = &final_page.checkpoint else {
            panic!("final history page must carry a change checkpoint");
        };
        let state = decode_gmail_state(&cursor.server_state).expect("checkpoint decodes");
        assert_eq!(state.history_id, 200);
    }

    /// A server that keeps handing back the same page token makes no
    /// progress, so the walk must stop. It must stop by TERMINATING:
    /// reporting a normal completion would emit a final checkpoint for a
    /// walk that never reached the end of the history, claiming coverage
    /// it did not earn.
    #[tokio::test]
    async fn a_repeated_history_page_token_terminates_the_walk() {
        let client = scripted_client(vec![
            ok_json(json!({ "emailAddress": "person@example.com", "historyId": "100" })),
            ok_json(json!({
                "historyId": "200",
                "history": [{"messagesAdded": [{"message": message("first", &[])}]}],
                "nextPageToken": "stuck"
            })),
            ok_json(json!({
                "historyId": "200",
                "history": [{"messagesAdded": [{"message": message("second", &[])}]}],
                "nextPageToken": "stuck"
            })),
        ]);
        let profile = GmailProfile {
            email_address: "person@example.com".to_string(),
            history_id: "100".to_string(),
        };

        let events = changes_stream(
            client,
            profile,
            cursor_for_history(100, "person@example.com"),
        )
        .collect::<Vec<_>>()
        .await;

        let SyncEvent::Batch(first) = &events[0] else {
            panic!("the first history page is delivered normally");
        };
        assert!(matches!(first.page_boundary, PageBoundary::Page));
        assert!(
            first.checkpoint.is_none(),
            "an intermediate page never checkpoints",
        );
        let SyncEvent::Terminated(error) = &events[1] else {
            panic!("a cycling page token must terminate, not complete");
        };
        assert!(matches!(
            error.kind(),
            bifrost_types::AccountErrorKind::Protocol(
                bifrost_types::ProtocolErrorKind::ContractViolation
            )
        ));
        assert!(
            !events
                .iter()
                .any(|event| matches!(event, SyncEvent::Done(_))),
            "a refused walk must not report completion",
        );
    }

    /// The budget is the second guard, for a server that cycles through
    /// FRESH tokens forever - repeated-token detection never fires there.
    #[test]
    fn the_page_budget_refuses_a_walk_that_never_ends() {
        let mut seen = HashSet::new();
        assert!(
            walk_refusal(&mut seen, MAX_HISTORY_PAGES - 1, Some("more")).is_none(),
            "the last page inside the budget still follows its token",
        );
        assert!(walk_refusal(&mut seen, MAX_HISTORY_PAGES, Some("another")).is_some());
        assert!(
            walk_refusal(&mut seen, MAX_HISTORY_PAGES, None).is_none(),
            "a walk that ends on the budget page ends normally",
        );
    }

    fn history(value: serde_json::Value) -> Vec<GmailHistoryItem> {
        serde_json::from_value(value).expect("history fixture deserializes")
    }

    fn ok_json(value: serde_json::Value) -> Canned {
        Canned::Response {
            status: StatusCode::OK,
            headers: reqwest::header::HeaderMap::new(),
            body: Bytes::from(serde_json::to_vec(&value).expect("fixture serializes")),
        }
    }

    fn scripted_client(steps: Vec<Canned>) -> Arc<GmailClient> {
        let script = ScriptedDispatch::new(steps);
        let net = bifrost_net::test_support::scripted_account(
            &script,
            NetConfig::default(),
            Vec::new(),
            Arc::new(StaticTokenSource::new("token", None)),
            RetryPolicy::disabled(),
        );
        Arc::new(GmailClient::with_account_net("https://gmail.test", net))
    }

    fn message(id: &str, labels: &[&str]) -> serde_json::Value {
        json!({
            "id": id,
            "threadId": format!("t-{id}"),
            "labelIds": labels,
        })
    }

    #[tokio::test]
    async fn changes_accepts_profile_email_case_drift() {
        let client = scripted_client(vec![
            ok_json(json!({ "emailAddress": "PERSON@example.com", "historyId": "100" })),
            ok_json(json!({ "historyId": "101", "history": [] })),
        ]);
        let profile = GmailProfile {
            email_address: "person@example.com".to_string(),
            history_id: "100".to_string(),
        };
        let cursor = cursor_for_history(100, "Person@Example.com");

        let events = changes_stream(client, profile, cursor)
            .collect::<Vec<_>>()
            .await;

        assert_eq!(events.len(), 2);
        let SyncEvent::Batch(batch) = &events[0] else {
            panic!("case-only profile drift must not terminate changes");
        };
        assert!(matches!(batch.page_boundary, PageBoundary::Final));
        assert!(matches!(events[1], SyncEvent::Done(None)));
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
