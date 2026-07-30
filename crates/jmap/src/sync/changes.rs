use std::num::NonZeroUsize;
use std::time::Instant;

use bifrost_types::{
    AccountStream, Batch, Change, ChangeCursor, Checkpoint, CursorScope,
    MailboxId as TypesMailboxId, ObjectChange, ObjectChangeKind, ObjectId, PageBoundary, SyncEvent,
};

use crate::core::changes::ChangesObject;
use crate::core::transport::HttpTransport;
use crate::email::{Email, EmailChanges};
use crate::mailbox::{Mailbox, MailboxChanges};

use super::capabilities::CoreLimits;
use super::state::{self, JmapScopeRepr};
use super::state_cache::{self, StateMap};

type MailAccount<T> = crate::account::Account<T>;

#[allow(clippy::too_many_arguments)]
pub(crate) fn stream<T: HttpTransport>(
    mail: MailAccount<T>,
    account_id: String,
    limits: CoreLimits,
    cursor: ChangeCursor,
    owner: Option<TypesMailboxId>,
    email_states: StateMap,
    mailbox_states: StateMap,
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
        // Thread cursor inventory is derived from Email inventory and is
        // deliberately not exposed. Old opaque Thread cursors must fail
        // explicitly rather than reviving an unseeded sync path.
        JmapScopeRepr::Thread => unsupported_scope(cursor.scope.clone(), "Thread"),
        // Query definitions are absent from the v1 Account contract, so a
        // query id cannot be turned into a valid filter/sort request. Do
        // not send an unfiltered Email/queryChanges and mislabel its ids.
        JmapScopeRepr::Query(_) => unsupported_scope(cursor.scope.clone(), "Query"),
        // A foreign (shared/delegate) account: its emails sync against
        // that account's `Email/changes` state. The owner tag drives
        // revocation isolation. `Email/changes` is account-wide and
        // cannot be filtered by mailbox, which is exactly why the seeded
        // topology is ONE account-level `Folder` scope per share: the
        // change set streams once, and per-mailbox membership is learned
        // at hydration via the foreign-qualified `mailboxIds` - the same
        // model the primary `Type(Email)` scope uses (no `ScopeChange`
        // needed). A legacy per-mailbox `Folder` cursor still lands here
        // and still advances correctly; it just is not seeded or
        // discovered anymore.
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
fn email_changes<T: HttpTransport>(
    mail: MailAccount<T>,
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
            // A foreign (shared/delegate) scope's change ids are qualified
            // with the owning accountId, matching what the foreign
            // inventory mints - otherwise hydrating a changed foreign email
            // would route through the primary account.
            let qualify = owner.as_ref().map(|owner| owner.0.clone());
            let changes = object_changes::<Email>(response.created(), ObjectChangeKind::Created, qualify.as_deref())
                .into_iter()
                .chain(object_changes::<Email>(response.updated(), ObjectChangeKind::Updated, qualify.as_deref()))
                .chain(object_changes::<Email>(
                    response.destroyed(),
                    ObjectChangeKind::Destroyed,
                    qualify.as_deref(),
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

fn mailbox_changes<T: HttpTransport>(
    mail: MailAccount<T>,
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
            let changes = object_changes::<Mailbox>(response.created(), ObjectChangeKind::Created, None)
                .into_iter()
                .chain(object_changes::<Mailbox>(
                    response.updated(),
                    ObjectChangeKind::Updated,
                    None,
                ))
                .chain(object_changes::<Mailbox>(
                    response.destroyed(),
                    ObjectChangeKind::Destroyed,
                    None,
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

fn unsupported_scope(scope: CursorScope, name: &'static str) -> AccountStream<SyncEvent<Change>> {
    Box::pin(async_stream::stream! {
        yield super::error::terminated(super::error::unsupported_error(
            bifrost_types::AccountOperation::SyncChanges,
            Some(bifrost_types::ErrorScope::Cursor(scope)),
            format!("JMAP {name} cursor changes are not supported by the v1 Account contract"),
        ));
    })
}

/// Project a changes id list onto `ObjectChange`s. `owner_account`
/// (`Some(accountId)` only for a foreign/shared scope) qualifies each id
/// into the owning account's namespace so downstream hydration and blob
/// reads route to that account; a primary scope leaves ids bare.
fn object_changes<O: ChangesObject>(
    ids: &[O::Id],
    kind: ObjectChangeKind,
    owner_account: Option<&str>,
) -> Vec<Change>
where
    O::Id: ToString,
{
    ids.iter()
        .map(|id| {
            let native = id.to_string();
            let id = match owner_account {
                Some(account) => super::foreign::encode_object(account, &native),
                None => native,
            };
            Change::ObjectChange(ObjectChange {
                id: ObjectId(id),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::email::EmailId;
    use futures::StreamExt;

    fn object_ids(changes: &[Change]) -> Vec<String> {
        changes
            .iter()
            .map(|change| match change {
                Change::ObjectChange(object) => object.id.0.clone(),
                other => panic!("expected an ObjectChange, got {other:?}"),
            })
            .collect()
    }

    #[test]
    fn primary_change_ids_stay_bare() {
        let ids = vec![EmailId::new("M1"), EmailId::new("M2")];
        let changes = object_changes::<Email>(&ids, ObjectChangeKind::Created, None);

        assert_eq!(object_ids(&changes), vec!["M1", "M2"]);
        for change in &changes {
            match change {
                Change::ObjectChange(object) => {
                    assert_eq!(object.kind, ObjectChangeKind::Created);
                }
                other => panic!("expected an ObjectChange, got {other:?}"),
            }
        }
    }

    #[test]
    fn foreign_change_ids_carry_their_owning_account() {
        // A foreign scope's change ids must land in the SAME namespace the
        // foreign inventory mints, or hydrating a changed shared-account
        // message routes through the primary account's `Email/get`.
        let ids = vec![EmailId::new("M1")];
        let changes = object_changes::<Email>(&ids, ObjectChangeKind::Destroyed, Some("acct-9"));

        assert_eq!(
            object_ids(&changes),
            vec![super::super::foreign::encode_object("acct-9", "M1")]
        );
        // And the encoding is reversible back to the wire id.
        assert_eq!(
            super::super::foreign::native_object(&object_ids(&changes)[0]),
            "M1"
        );
    }

    #[test]
    fn an_empty_change_list_yields_no_changes() {
        let ids: Vec<EmailId> = Vec::new();
        assert!(object_changes::<Email>(&ids, ObjectChangeKind::Updated, None).is_empty());
        assert!(
            object_changes::<Email>(&ids, ObjectChangeKind::Updated, Some("acct-9")).is_empty()
        );
    }

    #[test]
    fn every_change_stream_scope_can_build_its_checkpoint_cursor() {
        // `checkpoint_for` unwraps, so any scope reachable from a change
        // loop must encode. The foreign `Folder` shape is the one that
        // round-trips through a codec rather than a fixed tag.
        for scope in [
            CursorScope::Type(bifrost_types::ObjectType::Email),
            CursorScope::Type(bifrost_types::ObjectType::Mailbox),
            // The seeded foreign shape (account-level) and the legacy
            // per-mailbox shape both reach this loop; both must encode.
            CursorScope::Folder(super::super::foreign::encode_foreign_account("acct-9")),
            CursorScope::Folder(super::super::foreign::encode_foreign("acct-9", "mbx-1")),
        ] {
            let cursor = checkpoint_for(scope.clone(), "state-1".to_string());
            assert_eq!(cursor.scope, scope);
            assert_eq!(
                cursor.envelope_version,
                state::CHANGE_CURSOR_ENVELOPE_VERSION
            );
            let (_, state_string) =
                state::decode_cursor(&cursor).expect("a checkpoint must decode again");
            assert_eq!(state_string, "state-1");
        }
    }

    #[tokio::test]
    async fn query_changes_fail_before_an_unfiltered_request_can_be_sent() {
        let scope = CursorScope::Query(bifrost_types::QueryId("unread-in-inbox".to_string()));
        let mut stream = unsupported_scope(scope.clone(), "Query");

        match stream.next().await {
            Some(SyncEvent::Terminated(err)) => {
                assert_eq!(
                    err.kind(),
                    &bifrost_types::AccountErrorKind::Unsupported(
                        bifrost_types::AccountOperation::SyncChanges
                    )
                );
                assert_eq!(err.scope(), Some(&bifrost_types::ErrorScope::Cursor(scope)));
            }
            other => panic!("expected unsupported query termination, got {other:?}"),
        }
        assert!(stream.next().await.is_none());
    }
}
