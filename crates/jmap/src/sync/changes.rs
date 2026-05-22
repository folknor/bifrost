use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Instant;

use bifrost_types::{
    AccountStream, Batch, Change, ChangeCursor, Checkpoint, CursorScope, MembershipScope,
    ObjectChange, ObjectChangeKind, ObjectId, PageBoundary, ScopeChange, ScopeChangeKind,
    SyncEvent,
};
use tokio::sync::Mutex;

use crate::core::changes::ChangesObject;
use crate::core::query_changes::QueryChangesResponse;
use crate::email::{Email, EmailChanges, EmailQueryChanges};
use crate::mailbox::{Mailbox, MailboxChanges};
use crate::thread::{Thread, ThreadChanges};
use crate::transport_reqwest::ReqwestTransport;

use super::capabilities::CoreLimits;
use super::state::{self, JmapScopeRepr};

type MailAccount = crate::account::Account<ReqwestTransport>;

pub(crate) fn stream(
    mail: MailAccount,
    limits: CoreLimits,
    cursor: ChangeCursor,
    email_state: Arc<Mutex<Option<String>>>,
    mailbox_state: Arc<Mutex<Option<String>>>,
    thread_state: Arc<Mutex<Option<String>>>,
) -> AccountStream<SyncEvent<Change>> {
    let decoded = state::decode_cursor(&cursor);
    let (scope, state_string) = match decoded {
        Ok(decoded) => decoded,
        Err(err) => {
            return Box::pin(async_stream::stream! {
                yield super::error::fatal_from_account_error(
                    err,
                    Some(cursor.scope.clone()),
                    "failed to decode JMAP change cursor",
                );
            });
        }
    };

    match scope {
        JmapScopeRepr::Email => email_changes(
            mail,
            limits,
            cursor.scope.clone(),
            state_string,
            email_state,
        ),
        JmapScopeRepr::Mailbox => mailbox_changes(
            mail,
            limits,
            cursor.scope.clone(),
            state_string,
            mailbox_state,
        ),
        JmapScopeRepr::Thread => thread_changes(
            mail,
            limits,
            cursor.scope.clone(),
            state_string,
            thread_state,
        ),
        JmapScopeRepr::Query(query_id) => query_changes(mail, limits, query_id, state_string),
    }
}

fn email_changes(
    mail: MailAccount,
    limits: CoreLimits,
    scope: CursorScope,
    mut since_state: String,
    shared_state: Arc<Mutex<Option<String>>>,
) -> AccountStream<SyncEvent<Change>> {
    Box::pin(async_stream::stream! {
        let max_changes = nonzero(limits.max_objects_in_get);
        loop {
            let started = Instant::now();
            let response = mail
                .call(EmailChanges::new(since_state.clone()).max_changes(max_changes))
                .await;

            let response = match response {
                Ok(response) => response,
                Err(err) => {
                    yield super::error::fatal_from_jmap(err, Some(scope.clone()));
                    break;
                }
            };

            let new_state = response.new_state().to_string();
            let changes = object_changes::<Email>(response.created(), ObjectChangeKind::Created)
                .into_iter()
                .chain(object_changes::<Email>(response.updated(), ObjectChangeKind::Updated))
                .chain(object_changes::<Email>(
                    response.destroyed(),
                    ObjectChangeKind::Destroyed,
                ))
                .collect::<Vec<_>>();
            let checkpoint = checkpoint_for(scope.clone(), new_state.clone());
            advance_state(&shared_state, &since_state, new_state.clone()).await;

            yield SyncEvent::Batch(Batch {
                items: changes,
                page_boundary: PageBoundary::Page,
                server_latency: started.elapsed(),
                bytes_in: 0,
                checkpoint: Some(Checkpoint::Change(checkpoint.clone())),
            });

            since_state = new_state;
            if !response.has_more_changes() {
                yield SyncEvent::Done(Some(Checkpoint::Change(checkpoint)));
                break;
            }
        }
    })
}

fn mailbox_changes(
    mail: MailAccount,
    limits: CoreLimits,
    scope: CursorScope,
    mut since_state: String,
    shared_state: Arc<Mutex<Option<String>>>,
) -> AccountStream<SyncEvent<Change>> {
    Box::pin(async_stream::stream! {
        let max_changes = nonzero(limits.max_objects_in_get);
        loop {
            let started = Instant::now();
            let response = mail
                .call(MailboxChanges::new(since_state.clone()).max_changes(max_changes))
                .await;

            let response = match response {
                Ok(response) => response,
                Err(err) => {
                    yield super::error::fatal_from_jmap(err, Some(scope.clone()));
                    break;
                }
            };

            let new_state = response.new_state().to_string();
            let changes = object_changes::<Mailbox>(response.created(), ObjectChangeKind::Created)
                .into_iter()
                .chain(object_changes::<Mailbox>(
                    response.updated(),
                    ObjectChangeKind::Updated,
                ))
                .chain(object_changes::<Mailbox>(
                    response.destroyed(),
                    ObjectChangeKind::Destroyed,
                ))
                .collect::<Vec<_>>();
            let checkpoint = checkpoint_for(scope.clone(), new_state.clone());
            advance_state(&shared_state, &since_state, new_state.clone()).await;

            yield SyncEvent::Batch(Batch {
                items: changes,
                page_boundary: PageBoundary::Page,
                server_latency: started.elapsed(),
                bytes_in: 0,
                checkpoint: Some(Checkpoint::Change(checkpoint.clone())),
            });

            since_state = new_state;
            if !response.has_more_changes() {
                yield SyncEvent::Done(Some(Checkpoint::Change(checkpoint)));
                break;
            }
        }
    })
}

fn thread_changes(
    mail: MailAccount,
    limits: CoreLimits,
    scope: CursorScope,
    mut since_state: String,
    shared_state: Arc<Mutex<Option<String>>>,
) -> AccountStream<SyncEvent<Change>> {
    Box::pin(async_stream::stream! {
        let max_changes = nonzero(limits.max_objects_in_get);
        loop {
            let started = Instant::now();
            let response = mail
                .call(ThreadChanges::new(since_state.clone()).max_changes(max_changes))
                .await;

            let response = match response {
                Ok(response) => response,
                Err(err) => {
                    yield super::error::fatal_from_jmap(err, Some(scope.clone()));
                    break;
                }
            };

            let new_state = response.new_state().to_string();
            let changes = object_changes::<Thread>(response.created(), ObjectChangeKind::Created)
                .into_iter()
                .chain(object_changes::<Thread>(response.updated(), ObjectChangeKind::Updated))
                .chain(object_changes::<Thread>(
                    response.destroyed(),
                    ObjectChangeKind::Destroyed,
                ))
                .collect::<Vec<_>>();
            let checkpoint = checkpoint_for(scope.clone(), new_state.clone());
            advance_state(&shared_state, &since_state, new_state.clone()).await;

            yield SyncEvent::Batch(Batch {
                items: changes,
                page_boundary: PageBoundary::Page,
                server_latency: started.elapsed(),
                bytes_in: 0,
                checkpoint: Some(Checkpoint::Change(checkpoint.clone())),
            });

            since_state = new_state;
            if !response.has_more_changes() {
                yield SyncEvent::Done(Some(Checkpoint::Change(checkpoint)));
                break;
            }
        }
    })
}

fn query_changes(
    mail: MailAccount,
    limits: CoreLimits,
    query_id: String,
    since_state: String,
) -> AccountStream<SyncEvent<Change>> {
    Box::pin(async_stream::stream! {
        let started = Instant::now();
        let response = mail
            .call(EmailQueryChanges::new(since_state).max_changes(nonzero(limits.max_objects_in_get)))
            .await;

        let response: QueryChangesResponse<Email> = match response {
            Ok(response) => response,
            Err(err) => {
                yield super::error::fatal_from_jmap(
                    err,
                    Some(CursorScope::Query(bifrost_types::QueryId(query_id.clone()))),
                );
                return;
            }
        };

        let query_id = bifrost_types::QueryId(query_id);
        let scope = CursorScope::Query(query_id.clone());
        let membership = MembershipScope::Query(query_id);
        let removed = response.removed().iter().map(|id| {
            Change::ScopeChange(ScopeChange {
                id: ObjectId(id.to_string()),
                membership: membership.clone(),
                kind: ScopeChangeKind::Removed,
            })
        });
        let added = response.added().iter().map(|item| {
            Change::ScopeChange(ScopeChange {
                id: ObjectId(item.id().to_string()),
                membership: membership.clone(),
                kind: ScopeChangeKind::Added,
            })
        });
        let changes = removed.chain(added).collect::<Vec<_>>();
        let checkpoint = checkpoint_for(scope, response.new_query_state().to_string());

        yield SyncEvent::Batch(Batch {
            items: changes,
            page_boundary: PageBoundary::Page,
            server_latency: started.elapsed(),
            bytes_in: 0,
            checkpoint: Some(Checkpoint::Change(checkpoint.clone())),
        });
        yield SyncEvent::Done(Some(Checkpoint::Change(checkpoint)));
    })
}

fn object_changes<O: ChangesObject>(ids: &[O::Id], kind: ObjectChangeKind) -> Vec<Change>
where
    O::Id: ToString,
{
    ids.iter()
        .map(|id| {
            Change::ObjectChange(ObjectChange {
                id: ObjectId(id.to_string()),
                kind,
            })
        })
        .collect()
}

fn checkpoint_for(scope: CursorScope, state_string: String) -> ChangeCursor {
    state::cursor_for_scope(scope, state_string)
        .expect("JMAP change loops only checkpoint supported cursor scopes")
}

fn nonzero(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value.max(1)).expect("value.max(1) is non-zero")
}

async fn advance_state(state: &Arc<Mutex<Option<String>>>, expected: &str, value: String) {
    let mut guard = state.lock().await;
    match guard.as_deref() {
        Some(current) if current != expected => {}
        _ => *guard = Some(value),
    }
}
