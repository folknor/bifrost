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
                        changes.push(Change::ScopeChange(ScopeChange {
                            id: super::foreign::encode_message_id(&scope, &id.0),
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

            if !etags.is_empty() {
                let mut cache = account.etag_index.write().await;
                for (id, etag) in etags {
                    cache.insert(id, etag);
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
