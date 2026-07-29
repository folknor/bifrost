use bifrost_types::{
    AccountOperation, AccountStream, Change, ChangeCursor, Checkpoint, ErrorScope, ObjectChange,
    ObjectChangeKind, PageBoundary, ScopeChange, ScopeChangeKind, SyncEvent,
};
use serde_json::Value;

use super::GraphAccount;
use super::cursor::{CursorError, decode_cursor, encode_cursor};
use super::graph_error::{GraphErrorContext, cursor_error_to_account_error};
use super::inventory::{
    batch, fetch_delta_page, graph_etag, is_removed, membership_from_value, page_marker,
    removed_id, scope_matches_payload,
};

pub(crate) fn changes_stream(
    account: GraphAccount,
    cursor: ChangeCursor,
) -> AccountStream<SyncEvent<Change>> {
    Box::pin(async_stream::stream! {
        let mut payload = match decode_cursor(&cursor) {
            Ok(payload) => payload,
            Err(error) => {
                let ctx = GraphErrorContext::graph(AccountOperation::SyncChanges)
                    .with_scope(ErrorScope::Cursor(cursor.scope.clone()));
                yield SyncEvent::Terminated(cursor_error_to_account_error(error, ctx));
                yield SyncEvent::Done(None);
                return;
            }
        };
        if !scope_matches_payload(&cursor.scope, &payload) {
            let ctx = GraphErrorContext::graph(AccountOperation::SyncChanges)
                .with_scope(ErrorScope::Cursor(cursor.scope.clone()));
            yield SyncEvent::Terminated(cursor_error_to_account_error(
                CursorError::SchemaIncompatible,
                ctx,
            ));
            yield SyncEvent::Done(None);
            return;
        }

        let scope = cursor.scope.clone();
        let mut current_url = payload.resume_url().to_string();

        loop {
            let page = match fetch_delta_page(&account, &current_url).await {
                Ok(page) => page,
                Err(error) => {
                    let ctx = GraphErrorContext::graph(AccountOperation::SyncChanges)
                        .with_scope(ErrorScope::Cursor(scope.clone()));
                    // Foreign-scope permission denial quarantines just
                    // this scope; primary-scope denial stays terminal.
                    let owner = account.owner_of_scope(&scope);
                    yield SyncEvent::Terminated(super::graph_error::graph_shared_scope_error(
                        error,
                        &scope,
                        owner.as_ref(),
                        ctx,
                    ));
                    yield SyncEvent::Done(None);
                    return;
                }
            };
            let mut changes = Vec::new();
            let mut last_seen_id = None;
            let mut etags = Vec::new();
            let mut removed_etag_ids = Vec::new();
            for value in page.value {
                if let Some(id) = value.get("id").and_then(Value::as_str).map(str::to_string) {
                    last_seen_id = Some(id);
                }
                if is_removed(&value) {
                    if let Some(id) = removed_id(&value) {
                        // Encode the removed id the same way the Added /
                        // Updated ids are encoded, so a foreign item's
                        // remove carries the same bytes its add did
                        // (ratatoskr equality-joins on this id).
                        let id = super::foreign::encode_message_id(&scope, &id.0);
                        removed_etag_ids.push(id.0.clone());
                        changes.push(Change::ScopeChange(ScopeChange {
                            id,
                            membership: membership_from_value(&scope, &value),
                            kind: ScopeChangeKind::Removed,
                        }));
                    }
                    continue;
                }
                if let Some(id) = value.get("id").and_then(Value::as_str) {
                    // Foreign-encode the change id at mint: the readback
                    // guard feeds these ids straight back into
                    // `get_stream(FlagsOnly)`, which decodes and routes to
                    // `/users/{owner}`. A primary-scope id stays bare.
                    let object_id = super::foreign::encode_message_id(&scope, id);
                    if let Some(etag) = graph_etag(&value) {
                        etags.push((object_id.0.clone(), etag));
                    }
                    changes.push(Change::ObjectChange(ObjectChange {
                        id: object_id.clone(),
                        kind: ObjectChangeKind::Updated,
                    }));
                    changes.push(Change::ScopeChange(ScopeChange {
                        id: object_id,
                        membership: membership_from_value(&scope, &value),
                        kind: ScopeChangeKind::Added,
                    }));
                }
            }

            if !etags.is_empty() || !removed_etag_ids.is_empty() {
                let mut cache = account.etag_index.write().await;
                for (id, etag) in etags {
                    cache.insert(id, etag);
                }
                for id in removed_etag_ids {
                    cache.remove(&id);
                }
            }

            if let Some(next_link) = page.next_link {
                payload.advanced_through = Some(page_marker(next_link.clone(), last_seen_id));
                let checkpoint_cursor =
                    match encode_cursor(scope.clone(), payload.clone()) {
                        Ok(cursor) => cursor,
                        Err(error) => {
                            let ctx = GraphErrorContext::graph(AccountOperation::SyncChanges)
                                .with_scope(ErrorScope::Cursor(scope.clone()));
                            yield SyncEvent::Terminated(cursor_error_to_account_error(error, ctx));
                            yield SyncEvent::Done(None);
                            return;
                        }
                    };
                yield batch(changes, PageBoundary::Page, Some(checkpoint_cursor));
                current_url = next_link;
            } else if let Some(delta_link) = page.delta_link {
                payload.delta_link = delta_link;
                payload.advanced_through = None;
                let checkpoint_cursor =
                    match encode_cursor(scope.clone(), payload.clone()) {
                        Ok(cursor) => cursor,
                        Err(error) => {
                            let ctx = GraphErrorContext::graph(AccountOperation::SyncChanges)
                                .with_scope(ErrorScope::Cursor(scope.clone()));
                            yield SyncEvent::Terminated(cursor_error_to_account_error(error, ctx));
                            yield SyncEvent::Done(None);
                            return;
                        }
                    };
                let checkpoint = Checkpoint::Change(checkpoint_cursor.clone());
                yield batch(changes, PageBoundary::Final, Some(checkpoint_cursor));
                yield SyncEvent::Done(Some(checkpoint));
                return;
            } else {
                // A Graph delta page MUST carry either an `@odata.nextLink`
                // (more pages) or an `@odata.deltaLink` (end of the walk).
                // Neither present is a Graph contract violation. Emitting
                // `Final` + `Done(None)` here would drop the cursor advance,
                // so the engine would re-issue the same final page on every
                // poll forever. Terminate with a contract violation instead
                // so the failure is visible rather than a silent live-lock.
                if !changes.is_empty() {
                    yield batch(changes, PageBoundary::Page, None);
                }
                yield SyncEvent::Terminated(super::graph_error::protocol_violation(
                    bifrost_types::ProtocolErrorKind::ContractViolation,
                    AccountOperation::SyncChanges,
                    Some(ErrorScope::Cursor(scope.clone())),
                    "Graph delta page carried neither @odata.nextLink nor @odata.deltaLink",
                ));
                yield SyncEvent::Done(None);
                return;
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use bifrost_types::{
        AccountErrorKind, CursorScope, FolderId, ObjectType, OpaqueChangeState, ProtocolKind,
        SyncStateErrorKind,
    };
    use futures::StreamExt;

    use super::super::PushMode;
    use super::super::cursor::{
        CHANGE_CURSOR_ENVELOPE_VERSION, GRAPH_CURSOR_ENVELOPE_VERSION, GraphCursorPayload,
        encode_cursor, kind_for_scope,
    };
    use super::*;
    use crate::client::GraphClient;

    fn account() -> GraphAccount {
        GraphAccount::new_for_tests(GraphClient::new("token"), PushMode::GraphSubscriptions)
    }

    fn email_scope(folder: &str) -> CursorScope {
        CursorScope::FolderType {
            folder: FolderId(folder.to_string()),
            ty: ObjectType::Email,
        }
    }

    /// Drain a `changes_stream` that is expected to reject its cursor
    /// before touching the wire, returning the terminal error.
    async fn terminal_kind(cursor: ChangeCursor) -> bifrost_types::AccountError {
        let mut stream = changes_stream(account(), cursor);
        let error = match stream.next().await.expect("an event") {
            SyncEvent::Terminated(error) => error,
            _ => panic!("expected SyncEvent::Terminated"),
        };
        assert!(matches!(stream.next().await, Some(SyncEvent::Done(None))));
        assert!(stream.next().await.is_none());
        error
    }

    #[tokio::test]
    async fn a_cursor_minted_by_another_protocol_terminates_schema_incompatible() {
        // The engine persists cursors opaquely; handing a Gmail-tagged
        // cursor to Graph must be rejected at the door, not walked.
        let cursor = ChangeCursor {
            scope: email_scope("inbox"),
            server_state: OpaqueChangeState {
                protocol: ProtocolKind::Gmail,
                envelope_version: GRAPH_CURSOR_ENVELOPE_VERSION,
                bytes: Vec::new(),
            },
            advanced_through: None,
            envelope_version: CHANGE_CURSOR_ENVELOPE_VERSION,
        };
        let error = terminal_kind(cursor).await;
        assert!(matches!(
            error.kind(),
            AccountErrorKind::SyncState(SyncStateErrorKind::SchemaIncompatible)
        ));
    }

    #[tokio::test]
    async fn a_future_envelope_version_terminates_schema_incompatible() {
        let cursor = ChangeCursor {
            scope: email_scope("inbox"),
            server_state: OpaqueChangeState {
                protocol: ProtocolKind::Graph,
                envelope_version: GRAPH_CURSOR_ENVELOPE_VERSION + 1,
                bytes: b"{}".to_vec(),
            },
            advanced_through: None,
            envelope_version: CHANGE_CURSOR_ENVELOPE_VERSION,
        };
        let error = terminal_kind(cursor).await;
        assert!(matches!(
            error.kind(),
            AccountErrorKind::SyncState(SyncStateErrorKind::SchemaIncompatible)
        ));
    }

    #[tokio::test]
    async fn a_garbage_payload_terminates_as_a_contract_violation() {
        let cursor = ChangeCursor {
            scope: email_scope("inbox"),
            server_state: OpaqueChangeState {
                protocol: ProtocolKind::Graph,
                envelope_version: GRAPH_CURSOR_ENVELOPE_VERSION,
                bytes: b"{not-json".to_vec(),
            },
            advanced_through: None,
            envelope_version: CHANGE_CURSOR_ENVELOPE_VERSION,
        };
        let error = terminal_kind(cursor).await;
        assert!(matches!(
            error.kind(),
            AccountErrorKind::Protocol(bifrost_types::ProtocolErrorKind::ContractViolation)
        ));
    }

    #[tokio::test]
    async fn a_payload_naming_another_collection_terminates_before_any_fetch() {
        // The hostile / stale case that matters: a cursor whose scope says
        // "inbox messages" but whose delta link belongs to the contacts
        // collection. Walking it would file contact rows as messages, so
        // `scope_matches_payload` must terminate the stream.
        let contact_scope = CursorScope::FolderType {
            folder: FolderId("inbox".to_string()),
            ty: ObjectType::Contact,
        };
        let payload = GraphCursorPayload::new(
            kind_for_scope(&contact_scope).expect("contact scope maps"),
            "https://graph.example/delta".to_string(),
            None,
        );
        // `encode_cursor` does not cross-check, so the mismatch is
        // constructible exactly as a corrupted persisted cursor would be.
        let cursor = encode_cursor(email_scope("inbox"), payload).expect("encode");
        let error = terminal_kind(cursor).await;
        assert!(matches!(
            error.kind(),
            AccountErrorKind::SyncState(SyncStateErrorKind::SchemaIncompatible)
        ));
    }

    #[tokio::test]
    async fn a_payload_for_a_different_folder_terminates() {
        let payload = GraphCursorPayload::new(
            kind_for_scope(&email_scope("archive")).expect("email scope maps"),
            "https://graph.example/delta".to_string(),
            None,
        );
        let cursor = encode_cursor(email_scope("inbox"), payload).expect("encode");
        let error = terminal_kind(cursor).await;
        assert!(matches!(
            error.kind(),
            AccountErrorKind::SyncState(SyncStateErrorKind::SchemaIncompatible)
        ));
    }

    #[tokio::test]
    async fn the_event_calendar_event_alias_is_accepted_at_the_changes_door() {
        // A cursor minted from a `CalendarEvent` scope must not be rejected
        // when the engine hands it back tagged `Event` (the Events cursor
        // structurally cannot record which of the two it was). The stream
        // gets past the guard and fails on the wire instead - an unattached
        // client - which is the observable proof the guard let it through.
        let payload = GraphCursorPayload::new(
            kind_for_scope(&CursorScope::FolderType {
                folder: FolderId("calendar".to_string()),
                ty: ObjectType::CalendarEvent,
            })
            .expect("calendar event scope maps"),
            "https://graph.example/delta".to_string(),
            None,
        );
        let cursor = encode_cursor(
            CursorScope::FolderType {
                folder: FolderId("calendar".to_string()),
                ty: ObjectType::Event,
            },
            payload,
        )
        .expect("encode");
        let error = terminal_kind(cursor).await;
        assert!(
            !matches!(
                error.kind(),
                AccountErrorKind::SyncState(SyncStateErrorKind::SchemaIncompatible)
            ),
            "alias must not be rejected as a schema mismatch: {:?}",
            error.kind()
        );
    }
}
