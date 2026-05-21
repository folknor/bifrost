use bifrost_types::{
    Change, ChangeCursor, Checkpoint, ObjectChange, ObjectChangeKind, PageBoundary, RecoveryClass,
    ScopeChange, ScopeChangeKind, SyncEvent,
};
use serde_json::Value;

use super::GraphAccount;
use super::cursor::{decode_cursor, encode_cursor};
use super::error::{fatal_from_recovery, graph_error_to_fatal};
use super::inventory::{
    batch, fetch_delta_page, graph_etag, is_removed, membership_from_value, page_marker,
    removed_id, scope_matches_payload,
};

pub(crate) async fn change_events(
    account: GraphAccount,
    cursor: ChangeCursor,
) -> Vec<SyncEvent<Change>> {
    match changes_inner(&account, cursor).await {
        Ok(events) => events,
        Err(fatal) => vec![SyncEvent::Fatal(fatal), SyncEvent::Done(None)],
    }
}

async fn changes_inner(
    account: &GraphAccount,
    cursor: ChangeCursor,
) -> Result<Vec<SyncEvent<Change>>, bifrost_types::Fatal> {
    let mut payload = decode_cursor(&cursor).map_err(|error| {
        fatal_from_recovery(
            match error {
                bifrost_types::Error::CursorProtocolMismatch
                | bifrost_types::Error::CursorEnvelopeUnknown
                | bifrost_types::Error::SchemaIncompatible => RecoveryClass::SchemaIncompatible,
                _ => RecoveryClass::Fatal,
            },
            error.to_string(),
        )
    })?;
    if !scope_matches_payload(&cursor.scope, &payload) {
        return Err(fatal_from_recovery(
            RecoveryClass::SchemaIncompatible,
            "Graph cursor scope does not match cursor payload",
        ));
    }

    let scope = cursor.scope.clone();
    let mut current_url = payload.resume_url().to_string();
    let mut events = Vec::new();
    let mut etags = Vec::new();

    loop {
        let page = fetch_delta_page(account, &current_url)
            .await
            .map_err(|error| graph_error_to_fatal(error, scope.clone()))?;
        let mut changes = Vec::new();
        let mut last_seen_id = None;
        for value in page.value {
            if let Some(id) = value.get("id").and_then(Value::as_str).map(str::to_string) {
                last_seen_id = Some(id);
            }
            if is_removed(&value) {
                if let Some(id) = removed_id(&value) {
                    changes.push(Change::ScopeChange(ScopeChange {
                        id,
                        membership: membership_from_value(&scope, &value),
                        kind: ScopeChangeKind::Removed,
                    }));
                }
                continue;
            }
            if let Some(id) = value.get("id").and_then(Value::as_str) {
                let object_id = bifrost_types::ObjectId(id.to_string());
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

        if let Some(next_link) = page.next_link {
            payload.advanced_through = Some(page_marker(next_link.clone(), last_seen_id));
            let checkpoint_cursor = encode_cursor(scope.clone(), payload.clone())
                .map_err(|error| graph_error_to_fatal(error.to_string(), scope.clone()))?;
            events.push(batch(changes, PageBoundary::Page, Some(checkpoint_cursor)));
            current_url = next_link;
        } else if let Some(delta_link) = page.delta_link {
            payload.delta_link = delta_link;
            payload.advanced_through = None;
            let checkpoint_cursor = encode_cursor(scope.clone(), payload.clone())
                .map_err(|error| graph_error_to_fatal(error.to_string(), scope.clone()))?;
            let checkpoint = Checkpoint::Change(checkpoint_cursor.clone());
            events.push(batch(changes, PageBoundary::Final, Some(checkpoint_cursor)));
            events.push(SyncEvent::Done(Some(checkpoint)));
            break;
        } else {
            events.push(batch(changes, PageBoundary::Final, None));
            events.push(SyncEvent::Done(None));
            break;
        }
    }

    if !etags.is_empty() {
        let mut cache = account.etag_index.write().await;
        for (id, etag) in etags {
            cache.insert(id, etag);
        }
    }

    Ok(events)
}
