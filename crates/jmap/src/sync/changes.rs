use std::num::NonZeroUsize;
use std::time::Instant;

use bifrost_types::{
    AccountStream, Batch, Change, ChangeCursor, Checkpoint, CursorScope,
    MailboxId as TypesMailboxId, MembershipScope, ObjectChange, ObjectChangeKind, ObjectId,
    PageBoundary, ScopeChange, ScopeChangeKind, SyncEvent,
};

use crate::core::changes::ChangesObject;
use crate::core::query_changes::QueryChangesResponse;
use crate::email::{Email, EmailChanges, EmailQueryChanges};
use crate::mailbox::{Mailbox, MailboxChanges};
use crate::thread::{Thread, ThreadChanges};
use crate::transport_reqwest::ReqwestTransport;

use super::capabilities::CoreLimits;
use super::state::{self, JmapScopeRepr};
use super::state_cache::{self, StateMap};

type MailAccount = crate::account::Account<ReqwestTransport>;

#[allow(clippy::too_many_arguments)]
pub(crate) fn stream(
    mail: MailAccount,
    account_id: String,
    limits: CoreLimits,
    cursor: ChangeCursor,
    owner: Option<TypesMailboxId>,
    email_states: StateMap,
    mailbox_states: StateMap,
    thread_states: StateMap,
) -> AccountStream<SyncEvent<Change>> {
    let decoded = state::decode_cursor(&cursor);
    let (scope, state_string) = match decoded {
        Ok(decoded) => decoded,
        Err(err) => {
            return Box::pin(async_stream::stream! {
                // Cursor decode failure: the cursor envelope produced
                // by `state::decode_cursor` is a local cursor schema
                // error. `decode_cursor` reports it as a plain
                // `crate::Error` rather than a classified `AccountError`,
                // so we synthesize the SchemaIncompatible AccountError at
                // this boundary (the engine routes it to
                // `SchemaIncompatible`).
                let _ = err;
                yield super::error::terminated(
                    bifrost_types::AccountErrorBuilder::new(
                        bifrost_types::AccountErrorKind::SyncState(
                            bifrost_types::SyncStateErrorKind::SchemaIncompatible,
                        ),
                        bifrost_types::Cause::State(bifrost_types::StateCause::SchemaIncompatible),
                    )
                    .protocol(bifrost_types::Protocol::Jmap)
                    .operation(bifrost_types::AccountOperation::SyncChanges)
                    .scope(bifrost_types::ErrorScope::Cursor(cursor.scope.clone()))
                    .text(bifrost_types::DiagnosticText::support_only(
                        "failed to decode JMAP change cursor",
                    ))
                    .try_build()
                    .expect("valid account error classification"),
                );
            });
        }
    };

    match scope {
        JmapScopeRepr::Email => email_changes(
            mail,
            account_id,
            limits,
            cursor.scope.clone(),
            state_string,
            None,
            email_states,
        ),
        JmapScopeRepr::Mailbox => mailbox_changes(
            mail,
            account_id,
            limits,
            cursor.scope.clone(),
            state_string,
            mailbox_states,
        ),
        JmapScopeRepr::Thread => thread_changes(
            mail,
            account_id,
            limits,
            cursor.scope.clone(),
            state_string,
            thread_states,
        ),
        JmapScopeRepr::Query(query_id) => query_changes(mail, limits, query_id, state_string),
        // A foreign (shared/delegate) account mailbox: its emails sync
        // against that foreign account's `Email/changes` state. The
        // membership is the foreign mailbox; the owner tag drives
        // revocation isolation. `Email/changes` is account-wide, so the
        // per-mailbox membership rides on the change items' scope
        // changes rather than filtering the changes call.
        JmapScopeRepr::Folder { .. } => email_changes(
            mail,
            account_id,
            limits,
            cursor.scope.clone(),
            state_string,
            owner,
            email_states,
        ),
    }
}

#[allow(clippy::too_many_arguments)]
fn email_changes(
    mail: MailAccount,
    account_id: String,
    limits: CoreLimits,
    scope: CursorScope,
    mut since_state: String,
    owner: Option<TypesMailboxId>,
    email_states: StateMap,
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
                    // Foreign-scope permission denial quarantines just
                    // this scope; primary-scope denial stays terminal.
                    yield super::error::terminated(super::error::shared_scope_error(
                        err,
                        &scope,
                        owner.as_ref(),
                        super::error::JmapErrorContext::cursor(
                            bifrost_types::AccountOperation::SyncChanges,
                            scope.clone(),
                        ),
                    ));
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
            state_cache::advance(&email_states, &account_id, Some(&since_state), new_state.clone()).await;

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
    account_id: String,
    limits: CoreLimits,
    scope: CursorScope,
    mut since_state: String,
    mailbox_states: StateMap,
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
                    yield super::error::terminated_from_jmap(
                        err,
                        super::error::JmapErrorContext::cursor(
                            bifrost_types::AccountOperation::SyncChanges,
                            scope.clone(),
                        ),
                    );
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
            state_cache::advance(&mailbox_states, &account_id, Some(&since_state), new_state.clone()).await;

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
    account_id: String,
    limits: CoreLimits,
    scope: CursorScope,
    mut since_state: String,
    thread_states: StateMap,
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
                    yield super::error::terminated_from_jmap(
                        err,
                        super::error::JmapErrorContext::cursor(
                            bifrost_types::AccountOperation::SyncChanges,
                            scope.clone(),
                        ),
                    );
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
            state_cache::advance(&thread_states, &account_id, Some(&since_state), new_state.clone()).await;

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
                yield super::error::terminated_from_jmap(
                    err,
                    super::error::JmapErrorContext::cursor(
                        bifrost_types::AccountOperation::SyncChanges,
                        CursorScope::Query(bifrost_types::QueryId(query_id.clone())),
                    ),
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
