//! Retained operation identities over real driver transcripts.
use super::test_support::*;
use crate::types::{MailboxInfo, MailboxName, ModSeq, Uid, UidValidity};
use bifrost_types::{FlagOp, FlagSet, ItemOutcome, Projection, SyncEvent};
use futures::StreamExt;

fn deleted(name: MailboxName) -> MailboxInfo {
    MailboxInfo {
        name,
        attributes: vec![crate::types::MailboxAttribute::NonExistent],
        ..Default::default()
    }
}
fn shared_box() -> MailboxName {
    MailboxName::new("Shared/Box").unwrap()
}
fn shared_box_registry() -> FolderRegistry {
    FolderRegistry::from_lists(
        vec![],
        vec![crate::account::folder_registry::SharedFolderEntry {
            info: MailboxInfo {
                name: shared_box(),
                ..Default::default()
            },
            owner: bifrost_types::PrincipalId("alice".into()),
            rights: None,
            namespace_prefix: "Shared/".into(),
        }],
    )
}
fn epoch() -> UidValidity {
    UidValidity::new(1).unwrap()
}
fn seed(entry: &crate::account::folder_registry::FolderEntry, uid: u32, value: u64) {
    entry
        .record_modseq(epoch(), uid, ModSeq::new(value).unwrap())
        .unwrap();
}
fn recreate(account: &ImapAccount, folder: &MailboxName) {
    account.folders.apply_mailbox_event(deleted(folder.clone()));
    account.folders.apply_mailbox_event(MailboxInfo {
        name: folder.clone(),
        ..Default::default()
    });
}
fn ids(folder: &MailboxName) -> bifrost_types::AccountStream<bifrost_types::ObjectId> {
    Box::pin(futures::stream::iter([crate::account::encode_object_id(
        folder,
        epoch(),
        Uid::new(1).unwrap(),
    )]))
}
async fn selection(server: &mut tokio::io::DuplexStream, line: &str) {
    respond(server, &format!("{SELECT_PREAMBLE}* 1 EXISTS\r\n* OK [UIDVALIDITY 1] valid\r\n* OK [HIGHESTMODSEQ 100] modseq\r\n{} OK selected\r\n", tag_of(line))).await;
}

#[tokio::test]
async fn incarnation_hydration_records_only_on_original_entry() {
    let (conn, mut server) = driver_pair(&preauth_greeting("IMAP4rev1 CONDSTORE")).await;
    let account = scripted_sync_account(conn, 1, false, None, inbox_registry());
    let old = account.folders.get(&inbox()).unwrap();
    let script = async {
        let select = read_line(&mut server).await;
        selection(&mut server, &select).await;
        let fetch = read_line(&mut server).await;
        recreate(&account, &inbox());
        respond(
            &mut server,
            &format!(
                "* 1 FETCH (UID 1 FLAGS (\\Seen) MODSEQ (42))\r\n{} OK fetched\r\n",
                tag_of(&fetch)
            ),
        )
        .await;
    };
    let read =
        crate::account::get::get_stream(account.clone(), ids(&inbox()), Projection::FlagsOnly)
            .collect::<Vec<_>>();
    let (events, ()) = tokio::join!(read, script);
    assert!(events.iter().any(|e| matches!(e, SyncEvent::Batch(b) if matches!(b.items.as_slice(), [ItemOutcome::Succeeded(_)]))), "{events:?}");
    assert_eq!(old.modseq(epoch(), 1).map(ModSeq::get), Some(42));
    assert_eq!(
        account.folders.get(&inbox()).unwrap().modseq(epoch(), 1),
        None
    );
}

// The server reuses the epoch, so only the retained entry separates the
// mutation's guard and invalidation from the recreated mailbox's cache.
async fn mutation_cache(kind: crate::account::mutate::MutationKind, change_at_select: bool) {
    let (conn, mut server) =
        driver_pair(&preauth_greeting("IMAP4rev1 CONDSTORE UIDPLUS MOVE")).await;
    let account = scripted_sync_account(conn, 1, false, None, inbox_registry());
    let old = account.folders.get(&inbox()).unwrap();
    seed(&old, 1, 10);
    let destroy = matches!(kind, crate::account::mutate::MutationKind::Destroy);
    let two_stores = matches!(&kind, crate::account::mutate::MutationKind::Flags(FlagOp::Patch { add, remove }) if !add.is_empty() && !remove.is_empty());
    let script = async {
        let select = read_line(&mut server).await;
        if change_at_select {
            recreate(&account, &inbox());
            seed(&account.folders.get(&inbox()).unwrap(), 1, 90);
        }
        selection(&mut server, &select).await;
        let command = read_line(&mut server).await;
        if command.contains("STORE") {
            assert!(command.contains("UNCHANGEDSINCE 10"), "{command}");
        }
        if !change_at_select {
            recreate(&account, &inbox());
            seed(&account.folders.get(&inbox()).unwrap(), 1, 90);
        }
        respond(&mut server, &format!("{} OK applied\r\n", tag_of(&command))).await;
        if two_stores {
            let remove = read_line(&mut server).await;
            assert!(
                remove.contains("STORE") && remove.contains("-FLAGS"),
                "{remove}"
            );
            respond(&mut server, &format!("{} OK applied\r\n", tag_of(&remove))).await;
        }
        if destroy {
            let expunge = read_line(&mut server).await;
            assert!(expunge.contains("EXPUNGE"), "{expunge}");
            respond(
                &mut server,
                &format!("{} OK expunged\r\n", tag_of(&expunge)),
            )
            .await;
        }
    };
    let stream = crate::account::mutate::mutation_stream(account.clone(), ids(&inbox()), kind);
    let (events, ()) = tokio::join!(stream.collect::<Vec<_>>(), script);
    assert!(events.iter().any(|e| matches!(e, SyncEvent::Batch(b) if matches!(b.items.as_slice(), [ItemOutcome::Succeeded(_)]))), "{events:?}");
    assert_eq!(
        account
            .folders
            .get(&inbox())
            .unwrap()
            .modseq(epoch(), 1)
            .map(ModSeq::get),
        Some(90)
    );
    assert_eq!(old.modseq(epoch(), 1), None);
}
fn flags() -> crate::account::mutate::MutationKind {
    crate::account::mutate::MutationKind::Flags(FlagOp::Add(FlagSet::from(["\\Seen".into()])))
}
#[tokio::test]
async fn incarnation_mutation_reads_original_guard() {
    mutation_cache(flags(), true).await;
}
#[tokio::test]
async fn incarnation_flags_clear_original_cache() {
    mutation_cache(flags(), false).await;
}
#[tokio::test]
async fn incarnation_destroy_clears_original_cache() {
    mutation_cache(crate::account::mutate::MutationKind::Destroy, false).await;
}
#[tokio::test]
async fn incarnation_move_clears_original_cache() {
    mutation_cache(
        crate::account::mutate::MutationKind::Move(bifrost_types::MembershipScope::Folder(
            bifrost_types::FolderId("Archive".into()),
        )),
        false,
    )
    .await;
}

// A removed share keeps the command's original authority. Recreating its
// name as personal cannot turn a folder denial into an account failure.
async fn revoked_read_or_copy(copy: bool, preflight: bool) {
    let folder = shared_box();
    let (conn, mut server) = driver_pair(&preauth_greeting("IMAP4rev1 CONDSTORE")).await;
    let account = scripted_sync_account(conn, 1, false, None, shared_box_registry());
    let script = async {
        let select = read_line(&mut server).await;
        let command = if preflight {
            select
        } else {
            selection(&mut server, &select).await;
            read_line(&mut server).await
        };
        account.folders.apply_mailbox_event(deleted(folder.clone()));
        // A personal replacement deliberately has no inherited owner.
        account.folders.replace_personal(vec![MailboxInfo {
            name: folder.clone(),
            ..Default::default()
        }]);
        respond(
            &mut server,
            &format!("{} NO [NOPERM] revoked\r\n", tag_of(&command)),
        )
        .await;
    };
    let read = async {
        let errors: Vec<_> = if copy {
            crate::account::mutate::mutation_stream(
                account.clone(),
                ids(&folder),
                crate::account::mutate::MutationKind::Copy(MailboxName::new("Archive").unwrap()),
            )
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .flat_map(|e| match e {
                SyncEvent::Batch(b) => b
                    .items
                    .into_iter()
                    .filter_map(|i| match i {
                        ItemOutcome::Failed(f) => Some(f.error),
                        ItemOutcome::Uncertain(f) => Some(f.error),
                        _ => None,
                    })
                    .collect(),
                SyncEvent::Terminated(e) => vec![e],
                _ => vec![],
            })
            .collect()
        } else {
            crate::account::get::get_stream(account.clone(), ids(&folder), Projection::FlagsOnly)
                .collect::<Vec<_>>()
                .await
                .into_iter()
                .flat_map(|e| match e {
                    SyncEvent::Batch(b) => b
                        .items
                        .into_iter()
                        .filter_map(|i| match i {
                            ItemOutcome::Failed(f) => Some(f.error),
                            ItemOutcome::Uncertain(f) => Some(f.error),
                            _ => None,
                        })
                        .collect(),
                    _ => vec![],
                })
                .collect()
        };
        assert_eq!(errors.len(), 1, "{errors:?}");
        assert!(
            matches!(
                errors[0].kind(),
                AccountErrorKind::SyncState(bifrost_types::SyncStateErrorKind::ScopeRevoked)
            ),
            "{errors:?}"
        );
    };
    tokio::join!(read, script);
}
#[tokio::test]
async fn incarnation_hydration_retains_failure_owner() {
    revoked_read_or_copy(false, false).await;
}
#[tokio::test]
async fn incarnation_copy_retains_failure_owner() {
    revoked_read_or_copy(true, false).await;
}
#[tokio::test]
async fn incarnation_mutation_preflight_retains_failure_owner() {
    revoked_read_or_copy(true, true).await;
}

async fn push_cache(vanished: bool) {
    use bifrost_types::{Account, PushGate, PushLane, PushSessionEvent as E, SessionStop};
    let (account, peer, mut dials) = session_account(2, 1, false).await;
    account.folders.replace_personal(vec![MailboxInfo {
        name: inbox(),
        ..Default::default()
    }]);
    let old = account.folders.get(&inbox()).unwrap();
    seed(&old, 1, 10);
    account
        .push_subscribe(&[push_folder("INBOX")])
        .await
        .unwrap();
    let mut session = account.push_stream(PushLane(0), SessionStop::never(), PushGate::open());
    let setup = async {
        let mut server = dials.servers.recv().await.unwrap();
        let auth = read_line(&mut server).await;
        session_auth_with(
            &mut server,
            &auth,
            "IMAP4rev1 IDLE UNSELECT CONDSTORE QRESYNC",
        )
        .await;
        session_select(&mut server, "INBOX").await;
        server
    };
    let (_, mut server) = tokio::join!(
        next_events_until(&mut session, |e| matches!(e, E::Established)),
        setup
    );
    let script = async {
        let idle = read_line(&mut server).await;
        respond(&mut server, "+ idling\r\n").await;
        recreate(&account, &inbox());
        seed(&account.folders.get(&inbox()).unwrap(), 1, 90);
        respond(
            &mut server,
            if vanished {
                "* VANISHED 1\r\n"
            } else {
                "* 1 FETCH (UID 1 FLAGS (\\Seen) MODSEQ (42))\r\n"
            },
        )
        .await;
        let done = read_line(&mut server).await;
        assert!(done.contains("DONE"), "{done}");
        respond(&mut server, &format!("{} OK idle done\r\n", tag_of(&idle))).await;
    };
    tokio::join!(
        next_events_until(&mut session, |e| matches!(e, E::Healthy)),
        script
    );
    assert_eq!(
        account
            .folders
            .get(&inbox())
            .unwrap()
            .modseq(epoch(), 1)
            .map(ModSeq::get),
        Some(90)
    );
    assert_eq!(
        old.modseq(epoch(), 1).map(ModSeq::get),
        if vanished { None } else { Some(42) }
    );
    drop(session);
    drop(server);
    drop(peer);
    account.close().await.unwrap();
}
#[tokio::test]
async fn incarnation_push_fetch_records_original_entry() {
    push_cache(false).await;
}
#[tokio::test]
async fn incarnation_push_vanished_clears_original_entry() {
    push_cache(true).await;
}

#[tokio::test]
async fn incarnation_notify_retains_failure_owner() {
    use bifrost_types::{Account, PushGate, PushLane, PushSessionEvent as E, SessionStop};
    let (account, peer, mut dials) = session_account(2, 1, true).await;
    account
        .folders
        .ingest_shared(vec![crate::account::folder_registry::SharedFolderEntry {
            info: MailboxInfo {
                name: shared_box(),
                ..Default::default()
            },
            owner: bifrost_types::PrincipalId("alice".into()),
            rights: None,
            namespace_prefix: "Shared/".into(),
        }]);
    account
        .push_subscribe(&[push_folder("Shared/Box"), push_folder("Z")])
        .await
        .unwrap();
    let mut session = account.push_stream(PushLane(0), SessionStop::never(), PushGate::open());
    let setup = async {
        let mut server = dials.servers.recv().await.unwrap();
        let auth = read_line(&mut server).await;
        session_auth_with(&mut server, &auth, "IMAP4rev1 IDLE UNSELECT NOTIFY").await;
        session_select(&mut server, "Shared/Box").await;
        let notify = read_line(&mut server).await;
        assert!(notify.contains("NOTIFY SET"), "{notify}");
        account.folders.apply_mailbox_event(deleted(shared_box()));
        account.folders.replace_personal(vec![MailboxInfo {
            name: shared_box(),
            ..Default::default()
        }]);
        respond(
            &mut server,
            &format!("{} NO [NOPERM] revoked\r\n", tag_of(&notify)),
        )
        .await;
        server
    };
    let (events, server) = tokio::join!(session.by_ref().collect::<Vec<_>>(), setup);
    assert!(events.iter().any(|e| matches!(e, E::UnitFailed(error) if matches!(error.kind(), AccountErrorKind::SyncState(bifrost_types::SyncStateErrorKind::ScopeRevoked)))), "{events:?}");
    drop(session);
    drop(server);
    drop(peer);
    account.close().await.unwrap();
}

#[tokio::test]
async fn incarnation_patch_clears_original_cache() {
    // Both stores must stay with the same retained entry.
    mutation_cache(
        crate::account::mutate::MutationKind::Flags(FlagOp::Patch {
            add: FlagSet::from(["$flagged".into()]),
            remove: FlagSet::from(["$answered".into()]),
        }),
        false,
    )
    .await;
}

#[tokio::test]
async fn incarnation_absent_hydration_stays_absent() {
    let (conn, mut server) = driver_pair(&preauth_greeting("IMAP4rev1 CONDSTORE")).await;
    let account = scripted_account(conn, 1);
    let script = async {
        let select = read_line(&mut server).await;
        recreate(&account, &inbox());
        selection(&mut server, &select).await;
        let fetch = read_line(&mut server).await;
        respond(
            &mut server,
            &format!(
                "* 1 FETCH (UID 1 FLAGS () MODSEQ (42))\r\n{} OK fetched\r\n",
                tag_of(&fetch)
            ),
        )
        .await;
    };
    let read =
        crate::account::get::get_stream(account.clone(), ids(&inbox()), Projection::FlagsOnly)
            .collect::<Vec<_>>();
    tokio::join!(read, script);
    let entry = account.folders.get(&inbox()).unwrap();
    assert!(entry.last_seen().is_none());
    assert!(entry.modseq(epoch(), 1).is_none());
}

#[tokio::test]
async fn incarnation_copy_retains_destination_owner() {
    let (conn, mut server) = driver_pair(&preauth_greeting("IMAP4rev1 CONDSTORE")).await;
    let account = scripted_sync_account(conn, 1, false, None, shared_box_registry());
    account.folders.replace_personal(vec![MailboxInfo {
        name: inbox(),
        ..Default::default()
    }]);
    let script = async {
        let select = read_line(&mut server).await;
        selection(&mut server, &select).await;
        let copy = read_line(&mut server).await;
        account.folders.apply_mailbox_event(deleted(shared_box()));
        account.folders.replace_personal(vec![MailboxInfo {
            name: shared_box(),
            ..Default::default()
        }]);
        respond(
            &mut server,
            &format!("{} NO [NOPERM] revoked\r\n", tag_of(&copy)),
        )
        .await;
    };
    let read = crate::account::mutate::mutation_stream(
        account.clone(),
        ids(&inbox()),
        crate::account::mutate::MutationKind::Copy(shared_box()),
    )
    .collect::<Vec<_>>();
    let (events, ()) = tokio::join!(read, script);
    assert!(events.iter().any(|e| matches!(e, SyncEvent::Batch(b) if b.items.iter().any(|i| matches!(i, ItemOutcome::Failed(f) if matches!(f.error.kind(), AccountErrorKind::SyncState(bifrost_types::SyncStateErrorKind::ScopeRevoked)))))), "{events:?}");
}

#[tokio::test]
async fn incarnation_raw_read_retains_failure_owner() {
    let (conn, mut server) = driver_pair(&preauth_greeting("IMAP4rev1 CONDSTORE")).await;
    let account = scripted_sync_account(conn, 1, false, None, shared_box_registry());
    let message = crate::account::encode_object_id(&shared_box(), epoch(), Uid::new(1).unwrap());
    let script = async {
        let select = read_line(&mut server).await;
        selection(&mut server, &select).await;
        let fetch = read_line(&mut server).await;
        account.folders.apply_mailbox_event(deleted(shared_box()));
        account.folders.replace_personal(vec![MailboxInfo {
            name: shared_box(),
            ..Default::default()
        }]);
        respond(
            &mut server,
            &format!("{} NO [NOPERM] revoked\r\n", tag_of(&fetch)),
        )
        .await;
    };
    let read = crate::account::blob::open_raw_rfc822(account.clone(), message).collect::<Vec<_>>();
    let (events, ()) = tokio::join!(read, script);
    assert!(
        matches!(events.last(), Some(SyncEvent::Terminated(error)) if matches!(error.kind(), AccountErrorKind::SyncState(bifrost_types::SyncStateErrorKind::ScopeRevoked))),
        "{events:?}"
    );
}

#[tokio::test]
async fn incarnation_container_error_keeps_owner_before_checkout() {
    let (conn, mut server) = driver_pair(&preauth_greeting("IMAP4rev1")).await;
    let account = scripted_sync_account(conn, 1, false, None, shared_box_registry());
    let held = account.pool.checkout_any().await.unwrap();
    let mut operation = crate::account::containers::container_delete(
        account.clone(),
        ContainerId(shared_box().account_id()),
    );
    assert!(futures::poll!(&mut operation).is_pending());
    account.folders.apply_mailbox_event(deleted(shared_box()));
    account.folders.replace_personal(vec![MailboxInfo {
        name: shared_box(),
        ..Default::default()
    }]);
    drop(held);
    let script = async {
        let status = read_line(&mut server).await;
        assert!(status.contains("STATUS"), "{status}");
        respond(
            &mut server,
            &format!("{} NO [NOPERM] revoked\r\n", tag_of(&status)),
        )
        .await;
    };
    let (outcome, ()) = tokio::join!(operation, script);
    assert!(matches!(
        outcome.unwrap_err().kind(),
        AccountErrorKind::SyncState(bifrost_types::SyncStateErrorKind::ScopeRevoked)
    ));
}
