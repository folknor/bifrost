//! Byte-level account tests over the in-memory driver pair.
//!
//! These build a real `ImapAccount` around a `Pool` primed with a
//! `connection::test_support::driver_pair` connection, so an account
//! entry point is driven end to end against a canned server transcript.
//! Hermetic: `tokio::io::duplex`, no listener, no port, no daemon.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::Duration;

use bifrost_types::{ContainerId, ContainerKind};

use crate::connection::test_support::{driver_pair, preauth_greeting, read_line, respond, tag_of};
use crate::types::{AuthPolicy, Credentials};

use super::factory::ImapAccountConfig;
use super::folder_registry::FolderRegistry;
use super::{ImapAccount, ImapAccountParts, Pool};

/// Build an account whose pool holds exactly `pool_cap` permits and is
/// primed with the scripted connection. Every checkout therefore reuses
/// that one connection; a second concurrent checkout would have to dial,
/// which the duplex transport cannot do, so any nested acquire shows up
/// as a hang rather than silently passing.
fn scripted_account(conn: crate::ImapConnection, pool_cap: usize) -> ImapAccount {
    scripted_sync_account(conn, pool_cap, false, None, FolderRegistry::default())
}

/// `scripted_account` with the two knobs the sync half needs: whether
/// QRESYNC survived negotiation, and a registry pre-seeded with the folder
/// the transcript selects (an unregistered folder makes every cursor and
/// MODSEQ cache write a silent no-op).
fn scripted_sync_account(
    conn: crate::ImapConnection,
    pool_cap: usize,
    qresync_enabled: bool,
    qresync_negotiation_warning: Option<String>,
    folders: FolderRegistry,
) -> ImapAccount {
    let config = Arc::new(ImapAccountConfig {
        pool_cap,
        ..ImapAccountConfig::new(
            crate::ImapConfig::plaintext("test.invalid"),
            Credentials::password("user", "pass"),
            AuthPolicy::default(),
        )
    });
    let bandwidth_cap = Arc::new(AtomicU64::new(0));
    let pool = Arc::new(Pool::new(
        Arc::clone(&config),
        conn,
        pool_cap,
        None,
        Arc::clone(&bandwidth_cap),
    ));
    ImapAccount::new(ImapAccountParts {
        config,
        capabilities: super::test_support::stub_capabilities(),
        pool,
        folders: Arc::new(folders),
        qresync_enabled,
        qresync_negotiation_warning,
        supports_notify: false,
        bandwidth_cap,
        contacts: None,
        calendars: None,
        submission: None,
        dav_degraded: Vec::new(),
    })
}

/// A container mutation must complete with a single-permit pool.
///
/// `container_create` holds its pooled checkout across the post-mutation
/// `refresh_folders` re-LIST. If that refresh acquired a second permit,
/// this deadlocks forever at `pool_cap = 1` even though CREATE already
/// committed server-side. The timeout turns that regression into a fast
/// failure instead of a hung suite.
#[tokio::test]
async fn container_create_completes_under_a_single_permit_pool() {
    let (conn, mut server) = driver_pair(&preauth_greeting("IMAP4rev1")).await;
    let account = scripted_account(conn, 1);

    let script = tokio::spawn(async move {
        let create = read_line(&mut server).await;
        assert!(create.contains("CREATE"), "expected CREATE, got {create}");
        respond(
            &mut server,
            &format!("{} OK CREATE done\r\n", tag_of(&create)),
        )
        .await;

        // The re-LIST must arrive on the same connection the CREATE used.
        let list = read_line(&mut server).await;
        assert!(list.contains("LIST"), "expected LIST, got {list}");
        respond(
            &mut server,
            &format!(
                "* LIST (\\HasNoChildren) \".\" INBOX\r\n\
                 * LIST (\\HasNoChildren) \".\" Archive\r\n\
                 {} OK LIST done\r\n",
                tag_of(&list)
            ),
        )
        .await;
        server
    });

    let created = tokio::time::timeout(
        Duration::from_secs(5),
        super::pim::container_create(
            account.clone(),
            ContainerKind::Folder,
            "Archive".to_owned(),
            None,
            None,
        ),
    )
    .await
    .expect("container_create must not deadlock on the pool permit")
    .expect("scripted CREATE + LIST succeed");

    assert_eq!(created.0, "Archive");
    // The refresh landed: the re-LIST replaced the personal snapshot.
    assert!(account.folders.get(&created_name()).is_some());
    let _server = script.await.unwrap();
}

/// A hydration FETCH failure after the stale-UIDVALIDITY batch was
/// published must downgrade only the still-unresolved ids. Re-emitting the
/// stale ids as `Uncertain` would put one id in two lanes: the engine
/// would terminally fail it and simultaneously queue it for read-back.
#[tokio::test]
async fn folder_get_failure_does_not_relabel_published_stale_ids() {
    use bifrost_types::{ItemOutcome, Projection};

    let (conn, mut server) = driver_pair(&preauth_greeting("IMAP4rev1")).await;
    let account = scripted_account(conn, 1);
    let folder = crate::types::MailboxName::new("INBOX").unwrap();
    let stale_id = super::encode_object_id(&folder, 4, 7);
    let valid_id = super::encode_object_id(&folder, 5, 3);

    let script = tokio::spawn(async move {
        let select = read_line(&mut server).await;
        assert!(
            select.contains("EXAMINE") || select.contains("SELECT"),
            "expected a select, got {select}"
        );
        respond(
            &mut server,
            &format!(
                "* FLAGS (\\Answered \\Flagged \\Deleted \\Seen \\Draft)\r\n\
                 * 1 EXISTS\r\n\
                 * 0 RECENT\r\n\
                 * OK [UIDVALIDITY 5] ok\r\n\
                 * OK [UIDNEXT 9] ok\r\n\
                 {} OK [READ-ONLY] done\r\n",
                tag_of(&select)
            ),
        )
        .await;

        let fetch = read_line(&mut server).await;
        assert!(fetch.contains("FETCH"), "expected UID FETCH, got {fetch}");
        respond(
            &mut server,
            &format!("{} NO [UNAVAILABLE] busy\r\n", tag_of(&fetch)),
        )
        .await;
        server
    });

    let ids: bifrost_types::AccountStream<bifrost_types::ObjectId> =
        Box::pin(futures::stream::iter(vec![
            stale_id.clone(),
            valid_id.clone(),
        ]));
    let mut stream = super::get::get_stream(account, ids, Projection::Metadata);

    let mut failed = Vec::new();
    let mut uncertain = Vec::new();
    let mut succeeded = Vec::new();
    use futures::StreamExt;
    while let Some(event) = tokio::time::timeout(Duration::from_secs(5), stream.next())
        .await
        .expect("stream must make progress")
    {
        match event {
            SyncEvent::Batch(batch) => {
                for outcome in batch.items {
                    match outcome {
                        ItemOutcome::Failed(item) => failed.push(item.item.0.clone()),
                        ItemOutcome::Uncertain(item) => uncertain.push(item.item.0.clone()),
                        ItemOutcome::Succeeded(item) => succeeded.push(item.item.0.clone()),
                    }
                }
            }
            SyncEvent::Done(_) => break,
            other => panic!("unexpected event: {other:?}"),
        }
    }

    assert_eq!(
        failed,
        vec![stale_id.0.clone()],
        "the stale id fails exactly once, before the FETCH"
    );
    assert_eq!(
        uncertain,
        vec![valid_id.0.clone()],
        "only the unresolved id may fall to the uncertain lane"
    );
    assert!(succeeded.is_empty());
    let _server = script.await.unwrap();
}

fn created_name() -> crate::types::MailboxName {
    crate::types::MailboxName::new("Archive".to_owned()).unwrap()
}

/// The target buffer must flush at its exact ceiling even while the input
/// producer remains open. Removing the production flush makes this test time
/// out before the server sees SELECT.
#[tokio::test]
async fn get_flushes_at_target_buffer_boundary_before_input_closes() {
    use bifrost_types::{Projection, SyncEvent};
    use futures::StreamExt;

    let (conn, mut server) = driver_pair(&preauth_greeting("IMAP4rev1")).await;
    let account = scripted_account(conn, 1);
    let script = tokio::spawn(async move {
        let select = read_line(&mut server).await;
        assert!(
            select.contains("EXAMINE \"INBOX\""),
            "expected SELECT, got {select}"
        );
        respond(&mut server, &format!(
            "* FLAGS (\\Seen)\r\n* 256 EXISTS\r\n* 0 RECENT\r\n* OK [UIDVALIDITY 5] ok\r\n* OK [UIDNEXT 257] ok\r\n{} OK [READ-ONLY] done\r\n",
            tag_of(&select),
        )).await;
        let fetch = read_line(&mut server).await;
        assert!(
            fetch.contains("UID FETCH"),
            "expected bounded-window FETCH, got {fetch}"
        );
        respond(&mut server, &format!("{} OK done\r\n", tag_of(&fetch))).await;
    });

    let (input_tx, input_rx) = tokio::sync::mpsc::channel(257);
    for uid in 1..=super::TARGET_BUFFER_ITEMS {
        input_tx
            .send(super::encode_object_id(
                &crate::types::MailboxName::new("INBOX").unwrap(),
                5,
                u32::try_from(uid).unwrap(),
            ))
            .await
            .unwrap();
    }
    let ids = Box::pin(futures::stream::unfold(input_rx, |mut rx| async move {
        rx.recv().await.map(|item| (item, rx))
    }));
    let mut output = super::get::get_stream(account, ids, Projection::FlagsOnly);
    let first = tokio::time::timeout(Duration::from_secs(5), output.next())
        .await
        .expect("the full buffer must flush before input closes")
        .expect("one output batch");
    assert!(
        matches!(&first, SyncEvent::Batch(batch) if batch.items.iter().all(|item| !matches!(item, bifrost_types::ItemOutcome::Uncertain(_)))),
        "SELECT failed before the boundary FETCH: {first:?}"
    );
    drop(input_tx);
    script.await.unwrap();
}

#[tokio::test]
async fn mutation_flushes_at_target_buffer_boundary_before_input_closes() {
    use bifrost_types::{FlagOp, SyncEvent};
    use futures::StreamExt;

    let (conn, mut server) = driver_pair(&preauth_greeting("IMAP4rev1")).await;
    let account = scripted_account(conn, 1);
    let script = tokio::spawn(async move {
        let select = read_line(&mut server).await;
        assert!(
            select.contains("SELECT \"INBOX\""),
            "expected SELECT, got {select}"
        );
        respond(&mut server, &format!(
            "* FLAGS (\\Seen)\r\n* 256 EXISTS\r\n* 0 RECENT\r\n* OK [UIDVALIDITY 5] ok\r\n* OK [UIDNEXT 257] ok\r\n{} OK [READ-WRITE] done\r\n",
            tag_of(&select),
        )).await;
        let store = read_line(&mut server).await;
        assert!(
            store.contains("UID STORE"),
            "expected bounded-window STORE, got {store}"
        );
        respond(&mut server, &format!("{} OK done\r\n", tag_of(&store))).await;
    });

    let (input_tx, input_rx) = tokio::sync::mpsc::channel(257);
    for uid in 1..=super::TARGET_BUFFER_ITEMS {
        input_tx
            .send(super::encode_object_id(
                &crate::types::MailboxName::new("INBOX").unwrap(),
                5,
                u32::try_from(uid).unwrap(),
            ))
            .await
            .unwrap();
    }
    let ids = Box::pin(futures::stream::unfold(input_rx, |mut rx| async move {
        rx.recv().await.map(|item| (item, rx))
    }));
    let mut output = super::mutate::mutation_stream(
        account,
        ids,
        super::mutate::MutationKind::Flags(FlagOp::Add(std::collections::HashSet::from([
            "$flagged".to_owned(),
        ]))),
    );
    let first = tokio::time::timeout(Duration::from_secs(5), output.next())
        .await
        .expect("the full mutation buffer must flush before input closes")
        .expect("one output batch");
    assert!(
        matches!(&first, SyncEvent::Batch(batch) if batch.items.iter().all(|item| !matches!(item, bifrost_types::ItemOutcome::Uncertain(_)))),
        "SELECT failed before the boundary STORE: {first:?}"
    );
    drop(input_tx);
    script.await.unwrap();
}

/// A non-NOTIFY server gets one dedicated IDLE session per pushed folder, so
/// the fifth folder of a four-session budget cannot be pushed. It must be
/// refused in the failed lane rather than silently accepted: bifrost-sync
/// records only the succeeded scopes as pushed and keeps the rest polling, so
/// a silent acceptance would make that folder invisible - neither pushed nor
/// polled.
///
/// Pinned at the exact boundary in both directions. The fourth folder (item
/// "3") must succeed and the fifth (item "4") must fail; an off-by-one in
/// either direction moves one of those two.
#[tokio::test]
async fn non_notify_push_reports_the_folder_beyond_its_budget_as_poll_only() {
    use bifrost_types::{Account, CursorScope, FolderId};

    let (conn, server) = driver_pair(&preauth_greeting("IMAP4rev1")).await;
    let account = scripted_account(conn, 1);
    assert_eq!(
        account.config.idle_connection_budget, 4,
        "this transcript is written against the default budget",
    );
    let scopes: Vec<_> = ["A", "B", "C", "D", "E"]
        .into_iter()
        .map(|name| CursorScope::Folder(FolderId(name.to_owned())))
        .collect();
    let result = account
        .push_subscribe(&scopes)
        .await
        .expect("a refused scope is a failed item, never a whole-request Err");
    let succeeded: Vec<_> = result
        .outcomes
        .succeeded()
        .iter()
        .map(|item| item.item.0.clone())
        .collect();
    assert_eq!(
        succeeded,
        vec!["0", "1", "2", "3"],
        "every folder up to and including the budget is pushed",
    );
    assert_eq!(result.outcomes.failed().len(), 1);
    assert_eq!(
        result.outcomes.failed()[0].item.0,
        "4",
        "the first folder past the budget, and only it, is rejected",
    );
    assert!(
        result.outcomes.failed()[0]
            .error
            .user_safe_text()
            .any(|text| text.contains("budget")),
        "a capacity refusal must not read as an unsupported scope shape",
    );
    assert!(
        result.handle.is_some(),
        "accepted scopes retain one subscription handle"
    );
    // Workers are budget-many, not folder-many: the pool is started once and
    // idle slots park until a folder is admitted to them. The point of the
    // assertion is that a non-NOTIFY account runs the whole budget rather
    // than the single worker a NOTIFY account needs.
    assert_eq!(
        account.push.task_cancel.lock().unwrap().len(),
        account.config.idle_connection_budget,
        "a non-NOTIFY account runs one IDLE worker per budgeted session"
    );
    drop(server);
    account.close().await.unwrap();
}

/// DELETE must not be sent on a pooled connection that still has its target
/// selected. IMAP4rev2 provides UNSELECT, so this transcript proves the
/// affinity-free checkout explicitly deselects before STATUS/DELETE rather
/// than relying on a server accepting the selected-mailbox operation.
#[tokio::test]
async fn container_delete_deselects_a_target_left_selected_in_the_pool() {
    let (conn, mut server) = driver_pair(&preauth_greeting("IMAP4rev2")).await;
    let account = scripted_account(conn, 1);
    let folder = crate::types::MailboxName::new("Archive").unwrap();

    let script = tokio::spawn(async move {
        let select = read_line(&mut server).await;
        assert!(select.contains("SELECT"), "expected SELECT, got {select}");
        respond(
            &mut server,
            &format!(
                "* FLAGS (\\Answered \\Flagged \\Deleted \\Seen \\Draft)\r\n\
                 * 0 EXISTS\r\n\
                 * LIST () \"/\" Archive\r\n\
                 * OK [UIDVALIDITY 1] selected\r\n\
                 {} OK [READ-WRITE] SELECT done\r\n",
                tag_of(&select)
            ),
        )
        .await;

        let unselect = read_line(&mut server).await;
        assert!(
            unselect.contains("UNSELECT"),
            "expected UNSELECT before DELETE, got {unselect}"
        );
        respond(
            &mut server,
            &format!("{} OK UNSELECT done\r\n", tag_of(&unselect)),
        )
        .await;

        let status = read_line(&mut server).await;
        assert!(status.contains("STATUS"), "expected STATUS, got {status}");
        respond(
            &mut server,
            &format!(
                "* STATUS Archive (MESSAGES 0)\r\n{} OK STATUS done\r\n",
                tag_of(&status)
            ),
        )
        .await;

        let delete = read_line(&mut server).await;
        assert!(delete.contains("DELETE"), "expected DELETE, got {delete}");
        respond(
            &mut server,
            &format!("{} OK DELETE done\r\n", tag_of(&delete)),
        )
        .await;

        let list = read_line(&mut server).await;
        assert!(list.contains("LIST"), "expected LIST, got {list}");
        respond(&mut server, &format!("{} OK LIST done\r\n", tag_of(&list))).await;
        server
    });

    let mut pooled = account.pool.checkout_any().await.unwrap();
    account
        .select_folder(&mut pooled, &folder, None, false)
        .await
        .unwrap();
    drop(pooled);

    super::pim::container_delete(account.clone(), ContainerId("Archive".to_owned()))
        .await
        .expect("selected target must be deselected before delete");
    let _server = script.await.unwrap();
}

/// On a pre-UNSELECT server the deselect fallback replaces the checked-out
/// connection. It must release the old one BEFORE dialing the replacement:
/// dialing first holds `pool_cap + 1` physical connections across the whole
/// handshake, and a server enforcing its per-user connection limit rejects
/// exactly the fallback these old servers need. It also leaves the target
/// mailbox SELECTed on a live session while DELETE goes out (RFC 2683 2.2.2).
///
/// The pool is closed before the fallback runs, so the redial is refused
/// locally and cannot reach for a socket. What the transcript then proves is
/// the ordering: LOGOUT for the old member is on the wire even though the
/// replacement never happened.
#[tokio::test]
async fn deselect_fallback_releases_the_old_connection_before_redialing() {
    // IMAP4rev1 with no UNSELECT capability: `unselect` fails the gate
    // without touching the wire, so the fallback is what runs.
    let (conn, mut server) = driver_pair(&preauth_greeting("IMAP4rev1")).await;
    let account = scripted_account(conn, 1);
    let folder = crate::types::MailboxName::new("Archive").unwrap();

    let script = tokio::spawn(async move {
        let select = read_line(&mut server).await;
        assert!(select.contains("SELECT"), "expected SELECT, got {select}");
        respond(
            &mut server,
            &format!(
                "* FLAGS (\\Deleted \\Seen)\r\n\
                 * 0 EXISTS\r\n\
                 * 0 RECENT\r\n\
                 * OK [UIDVALIDITY 1] selected\r\n\
                 {} OK [READ-WRITE] SELECT done\r\n",
                tag_of(&select)
            ),
        )
        .await;

        let logout = read_line(&mut server).await;
        assert!(
            logout.contains("LOGOUT"),
            "the superseded selected connection must be closed first, got {logout}"
        );
        respond(
            &mut server,
            &format!("* BYE closing\r\n{} OK LOGOUT done\r\n", tag_of(&logout)),
        )
        .await;
        server
    });

    let mut pooled = account.pool.checkout_any().await.unwrap();
    account
        .select_folder(&mut pooled, &folder, None, false)
        .await
        .unwrap();

    // Refuse the redial locally rather than letting it look for a socket.
    account.pool.close().await;

    let result = pooled
        .deselect_target(&folder, Duration::from_secs(5))
        .await;
    assert!(
        result.is_err(),
        "a refused redial must surface, not silently leave the target selected"
    );
    let _server = tokio::time::timeout(Duration::from_secs(5), script)
        .await
        .expect("LOGOUT must precede the replacement dial")
        .unwrap();
}

// ---------------------------------------------------------------------------
// Sync half: strategy dispatch, VANISHED/FETCH dedup, downgrades, checkpoints.
//
// Every test below drives the real `changes_stream` over a canned transcript,
// so the strategy a cursor variant selects is proven by the bytes that reach
// the server, not by reading the dispatch match.
// ---------------------------------------------------------------------------

use bifrost_types::{
    Change, ChangeCursor, Checkpoint, DiagnosticText, ObjectChange, ObjectChangeKind, PageBoundary,
    ScopeChange, ScopeChangeKind, SyncEvent, SyncStrategy, Warning, WarningKind,
};

use crate::types::MailboxInfo;

use super::{CompactUidSet, FolderCursor};

fn inbox() -> crate::types::MailboxName {
    crate::types::MailboxName::new("INBOX").unwrap()
}

/// A registry holding just INBOX, so cursor and MODSEQ cache writes land.
/// An unregistered folder makes every registry write a silent no-op.
fn inbox_registry() -> FolderRegistry {
    FolderRegistry::from_list(vec![MailboxInfo {
        name: inbox(),
        ..MailboxInfo::default()
    }])
}

/// The mandatory SELECT/EXAMINE preamble (RFC 3501 Sections 6.3.1-6.3.2).
///
/// `SelectConsumer` validates FLAGS + EXISTS + RECENT before it reads a
/// single response code, so a transcript that omits them fails the command
/// with `Error::Protocol` and the whole changes stream terminates before
/// any strategy work runs. Every select transcript below starts here.
const SELECT_PREAMBLE: &str =
    "* FLAGS (\\Answered \\Flagged \\Deleted \\Seen \\Draft)\r\n* 0 RECENT\r\n";

fn cursor_for(cursor: &FolderCursor) -> ChangeCursor {
    super::encode_cursor(super::folder_scope(&inbox()), cursor)
}

/// Drive `changes_stream` to completion, bounded so a regression that stalls
/// the pipeline fails fast instead of hanging the suite.
async fn collect_changes(account: ImapAccount, cursor: ChangeCursor) -> Vec<SyncEvent<Change>> {
    use futures::StreamExt;

    let mut stream = super::changes::changes_stream(account, cursor);
    let mut events = Vec::new();
    loop {
        let next = tokio::time::timeout(Duration::from_secs(5), stream.next())
            .await
            .expect("changes stream must make progress");
        let Some(event) = next else { break };
        let terminal = matches!(event, SyncEvent::Done(_) | SyncEvent::Terminated(_));
        events.push(event);
        if terminal {
            break;
        }
    }
    events
}

/// The change lanes flattened in emission order: `(object id, lane)`.
fn change_labels(events: &[SyncEvent<Change>]) -> Vec<(String, &'static str)> {
    let mut out = Vec::new();
    for event in events {
        let SyncEvent::Batch(batch) = event else {
            continue;
        };
        for change in &batch.items {
            match change {
                Change::ScopeChange(ScopeChange {
                    id,
                    kind: ScopeChangeKind::Added,
                    ..
                }) => out.push((id.0.clone(), "added")),
                Change::ScopeChange(ScopeChange {
                    id,
                    kind: ScopeChangeKind::Removed,
                    ..
                }) => out.push((id.0.clone(), "removed")),
                Change::ObjectChange(ObjectChange {
                    id,
                    kind: ObjectChangeKind::Updated,
                }) => out.push((id.0.clone(), "updated")),
                other => panic!("unexpected change: {other:?}"),
            }
        }
    }
    out
}

fn warnings_of(events: &[SyncEvent<Change>]) -> Vec<&Warning> {
    events
        .iter()
        .filter_map(|event| match event {
            SyncEvent::Warning(warning) => Some(warning),
            _ => None,
        })
        .collect()
}

/// The cursor carried by the terminal `Done`, decoded back to a `FolderCursor`.
fn done_cursor(events: &[SyncEvent<Change>]) -> FolderCursor {
    let last = events.last().expect("stream emits a terminal event");
    let SyncEvent::Done(checkpoint) = last else {
        panic!("expected Done, got {last:?}");
    };
    let Some(Checkpoint::Change(cursor)) = checkpoint else {
        panic!("changes must checkpoint a change cursor");
    };
    super::decode_cursor(cursor).expect("emitted checkpoint must decode")
}

fn id(uidvalidity: u32, uid: u32) -> String {
    super::encode_object_id(&inbox(), uidvalidity, uid).0
}

fn downgrade_detail(from: SyncStrategy, to: SyncStrategy) -> String {
    format!("{from:?}->{to:?}")
}

/// QRESYNC happy path.
///
/// Pins four things at once: the QRESYNC cursor variant puts the QRESYNC
/// parameter (with the known-UID baseline) on the SELECT line; the same UID
/// reported by SELECT-side VANISHED and then by the CHANGEDSINCE FETCH
/// collapses into a single `Updated` instead of surfacing in both the expunge
/// and the update lane; a UID repeated across FETCH responses is emitted once;
/// and the final checkpoint carries the server's new HIGHESTMODSEQ plus the
/// recomputed live UID set.
#[tokio::test]
async fn qresync_dedupes_vanished_against_fetch_and_checkpoints_the_live_set() {
    let (conn, mut server) =
        driver_pair(&preauth_greeting("IMAP4rev1 ENABLE CONDSTORE QRESYNC")).await;
    let account = scripted_sync_account(conn, 1, true, None, inbox_registry());

    let script = tokio::spawn(async move {
        let enable = read_line(&mut server).await;
        assert!(
            enable.contains("ENABLE QRESYNC"),
            "QRESYNC must be ENABLEd before the SELECT that uses it, got {enable}"
        );
        respond(
            &mut server,
            &format!(
                "* ENABLED QRESYNC\r\n{} OK ENABLE done\r\n",
                tag_of(&enable)
            ),
        )
        .await;

        let select = read_line(&mut server).await;
        assert!(
            select.contains("EXAMINE") && select.contains("QRESYNC (5 100 1:3)"),
            "a QRESYNC cursor must select with its uidvalidity/modseq/known-uid \
             baseline, got {select}"
        );
        respond(
            &mut server,
            &format!(
                "{SELECT_PREAMBLE}\
                 * 2 EXISTS\r\n\
                 * OK [UIDVALIDITY 5] ok\r\n\
                 * OK [UIDNEXT 9] ok\r\n\
                 * OK [HIGHESTMODSEQ 200] ok\r\n\
                 * VANISHED (EARLIER) 2\r\n\
                 * 1 FETCH (UID 3 FLAGS (\\Seen) MODSEQ (150))\r\n\
                 {} OK [READ-ONLY] EXAMINE done\r\n",
                tag_of(&select)
            ),
        )
        .await;

        let fetch = read_line(&mut server).await;
        assert!(
            fetch.contains("CHANGEDSINCE 100") && fetch.contains("VANISHED"),
            "QRESYNC diffs with CHANGEDSINCE ... VANISHED, got {fetch}"
        );
        respond(
            &mut server,
            &format!(
                "* 1 FETCH (UID 2 FLAGS (\\Deleted) MODSEQ (180))\r\n\
                 * 2 FETCH (UID 3 FLAGS (\\Seen) MODSEQ (150))\r\n\
                 * VANISHED (EARLIER) 1\r\n\
                 {} OK UID FETCH done\r\n",
                tag_of(&fetch)
            ),
        )
        .await;
        server
    });

    let events = collect_changes(
        account,
        cursor_for(&FolderCursor::QResync {
            uidvalidity: 5,
            modseq: 100,
            known_uids: CompactUidSet::from_uids([1, 2, 3]),
            known_uids_complete: true,
        }),
    )
    .await;

    assert!(
        warnings_of(&events).is_empty(),
        "a clean QRESYNC round must not warn: {:?}",
        warnings_of(&events)
    );
    assert_eq!(
        change_labels(&events),
        vec![
            (id(5, 3), "updated"),
            (id(5, 2), "updated"),
            (id(5, 1), "removed"),
        ],
        "UID 2 was VANISHED then re-FETCHed, so it must land in one lane only, \
         and the twice-FETCHed UID 3 must be emitted once"
    );

    match done_cursor(&events) {
        FolderCursor::QResync {
            uidvalidity,
            modseq,
            known_uids,
            known_uids_complete,
        } => {
            assert_eq!((uidvalidity, modseq), (5, 200));
            assert_eq!(known_uids.to_uids(), vec![2, 3]);
            assert!(known_uids_complete);
        }
        other => panic!("QRESYNC must checkpoint a QRESYNC cursor, got {other:?}"),
    }
    let _server = script.await.unwrap();
}

/// A QRESYNC cursor on a session where QRESYNC did not survive negotiation
/// runs CONDSTORE instead, and says so with the specific negotiation reason
/// rather than the generic fallback text. The cursor also lacks a complete UID
/// baseline, so the CONDSTORE run seeds from `UID SEARCH ALL` *before* the
/// CHANGEDSINCE FETCH and then reuses that snapshot as the live set: one
/// SEARCH, not two, and no spurious added/removed churn on the seeding round.
#[tokio::test]
async fn qresync_cursor_without_negotiation_downgrades_to_condstore_and_seeds_the_baseline() {
    let (conn, mut server) = driver_pair(&preauth_greeting("IMAP4rev1 CONDSTORE")).await;
    let account = scripted_sync_account(
        conn,
        1,
        false,
        Some("server did not echo ENABLED QRESYNC".to_owned()),
        inbox_registry(),
    );

    let script = tokio::spawn(async move {
        let select = read_line(&mut server).await;
        assert!(
            select.contains("EXAMINE") && !select.contains("QRESYNC"),
            "a downgraded session must not put QRESYNC on the wire, got {select}"
        );
        respond(
            &mut server,
            &format!(
                "{SELECT_PREAMBLE}\
                 * 3 EXISTS\r\n\
                 * OK [UIDVALIDITY 5] ok\r\n\
                 * OK [UIDNEXT 9] ok\r\n\
                 * OK [HIGHESTMODSEQ 200] ok\r\n\
                 {} OK [READ-ONLY] EXAMINE done\r\n",
                tag_of(&select)
            ),
        )
        .await;

        let search = read_line(&mut server).await;
        assert!(
            search.contains("UID SEARCH ALL"),
            "the partial baseline is seeded before the diff, got {search}"
        );
        respond(
            &mut server,
            &format!(
                "* SEARCH 2 3 4\r\n{} OK UID SEARCH done\r\n",
                tag_of(&search)
            ),
        )
        .await;

        let fetch = read_line(&mut server).await;
        assert!(
            fetch.contains("CHANGEDSINCE 100") && !fetch.contains("VANISHED"),
            "CONDSTORE diffs with CHANGEDSINCE and no VANISHED modifier, got {fetch}"
        );
        respond(
            &mut server,
            &format!(
                "* 3 FETCH (UID 4 FLAGS (\\Seen) MODSEQ (190))\r\n\
                 {} OK UID FETCH done\r\n",
                tag_of(&fetch)
            ),
        )
        .await;
        server
    });

    let events = collect_changes(
        account,
        cursor_for(&FolderCursor::QResync {
            uidvalidity: 5,
            modseq: 100,
            known_uids: CompactUidSet::from_uids([1, 2]),
            known_uids_complete: false,
        }),
    )
    .await;

    let warnings = warnings_of(&events);
    assert_eq!(
        warnings.len(),
        2,
        "expected downgrade + seeding, got {warnings:?}"
    );
    assert_eq!(warnings[0].kind, WarningKind::StrategyDowngraded);
    assert_eq!(
        warnings[0].message.as_str(),
        "server did not echo ENABLED QRESYNC",
        "the downgrade must name the actual negotiation failure, not the generic fallback"
    );
    assert_eq!(
        warnings[0]
            .protocol_detail
            .as_ref()
            .map(DiagnosticText::as_str),
        Some(downgrade_detail(SyncStrategy::QResync, SyncStrategy::Condstore).as_str())
    );
    assert_eq!(warnings[1].kind, WarningKind::Other);

    assert_eq!(
        change_labels(&events),
        vec![(id(5, 4), "updated")],
        "the seeded snapshot is the baseline, so it must not diff against the stale known set"
    );
    match done_cursor(&events) {
        FolderCursor::Condstore {
            uidvalidity,
            modseq,
            known_uids,
        } => {
            assert_eq!((uidvalidity, modseq), (5, 200));
            assert_eq!(known_uids.to_uids(), vec![2, 3, 4]);
        }
        other => panic!("a CONDSTORE run must checkpoint a CONDSTORE cursor, got {other:?}"),
    }
    let _server = script.await.unwrap();
}

/// CONDSTORE with a complete baseline: flags come from CHANGEDSINCE, expunges
/// and arrivals from diffing `UID SEARCH ALL` against the cursor's UID set.
/// The SEARCH follows the FETCH here (unlike the seeding round above), which
/// is what makes the checkpointed UID snapshot current.
#[tokio::test]
async fn condstore_cursor_diffs_changedsince_flags_against_a_uid_search_snapshot() {
    let (conn, mut server) = driver_pair(&preauth_greeting("IMAP4rev1 CONDSTORE")).await;
    let account = scripted_sync_account(conn, 1, false, None, inbox_registry());

    let script = tokio::spawn(async move {
        let select = read_line(&mut server).await;
        assert!(
            select.contains("EXAMINE") && select.contains("CONDSTORE"),
            "a CONDSTORE cursor selects with the CONDSTORE parameter, got {select}"
        );
        respond(
            &mut server,
            &format!(
                "{SELECT_PREAMBLE}\
                 * 3 EXISTS\r\n\
                 * OK [UIDVALIDITY 7] ok\r\n\
                 * OK [UIDNEXT 12] ok\r\n\
                 * OK [HIGHESTMODSEQ 60] ok\r\n\
                 {} OK [READ-ONLY] EXAMINE done\r\n",
                tag_of(&select)
            ),
        )
        .await;

        let fetch = read_line(&mut server).await;
        assert!(
            fetch.contains("CHANGEDSINCE 50"),
            "expected CHANGEDSINCE against the cursor modseq, got {fetch}"
        );
        respond(
            &mut server,
            &format!(
                "* 1 FETCH (UID 2 FLAGS (\\Seen) MODSEQ (55))\r\n\
                 {} OK UID FETCH done\r\n",
                tag_of(&fetch)
            ),
        )
        .await;

        let search = read_line(&mut server).await;
        assert!(
            search.contains("UID SEARCH ALL"),
            "CONDSTORE detects expunges by UID-list diff, got {search}"
        );
        respond(
            &mut server,
            &format!(
                "* SEARCH 2 3 4\r\n{} OK UID SEARCH done\r\n",
                tag_of(&search)
            ),
        )
        .await;
        server
    });

    let events = collect_changes(
        account,
        cursor_for(&FolderCursor::Condstore {
            uidvalidity: 7,
            modseq: 50,
            known_uids: CompactUidSet::from_uids([1, 2, 3]),
        }),
    )
    .await;

    assert!(
        warnings_of(&events).is_empty(),
        "{:?}",
        warnings_of(&events)
    );
    assert_eq!(
        change_labels(&events),
        vec![
            (id(7, 2), "updated"),
            (id(7, 4), "added"),
            (id(7, 1), "removed"),
        ]
    );
    match done_cursor(&events) {
        FolderCursor::Condstore {
            uidvalidity,
            modseq,
            known_uids,
        } => {
            assert_eq!((uidvalidity, modseq), (7, 60));
            assert_eq!(known_uids.to_uids(), vec![2, 3, 4]);
        }
        other => panic!("expected a CONDSTORE checkpoint, got {other:?}"),
    }
    let _server = script.await.unwrap();
}

/// A server that advertises QRESYNC but not ENABLE fails the ENABLE leg of
/// `select_for_sync` with `MissingCapability("ENABLE")` before writing a
/// byte. That must downgrade to CONDSTORE, not terminate the stream, and
/// the retry must not deadlock on the pool permit the failed QRESYNC
/// attempt still holds: at `pool_cap = 1` this test hangs (and fails on
/// `collect_changes`' timeout) if the checkout is not released before the
/// retry re-enters `checkout_for_folder`.
#[tokio::test]
async fn qresync_without_enable_capability_downgrades_and_does_not_deadlock() {
    let (conn, mut server) = driver_pair(&preauth_greeting("IMAP4rev1 QRESYNC CONDSTORE")).await;
    let account = scripted_sync_account(conn, 1, true, None, inbox_registry());

    let script = tokio::spawn(async move {
        // The failed QRESYNC leg writes nothing: the first command on the
        // wire is already the CONDSTORE retry's EXAMINE.
        let select = read_line(&mut server).await;
        assert!(
            select.contains("EXAMINE")
                && select.contains("CONDSTORE")
                && !select.contains("QRESYNC"),
            "expected the CONDSTORE retry's EXAMINE as the first command, got {select}"
        );
        respond(
            &mut server,
            &format!(
                "{SELECT_PREAMBLE}\
                 * 3 EXISTS\r\n\
                 * OK [UIDVALIDITY 7] ok\r\n\
                 * OK [UIDNEXT 12] ok\r\n\
                 * OK [HIGHESTMODSEQ 60] ok\r\n\
                 {} OK [READ-ONLY] EXAMINE done\r\n",
                tag_of(&select)
            ),
        )
        .await;

        let fetch = read_line(&mut server).await;
        assert!(
            fetch.contains("CHANGEDSINCE 50"),
            "the retry diffs against the QRESYNC cursor's modseq, got {fetch}"
        );
        respond(
            &mut server,
            &format!(
                "* 1 FETCH (UID 2 FLAGS (\\Seen) MODSEQ (55))\r\n\
                 {} OK UID FETCH done\r\n",
                tag_of(&fetch)
            ),
        )
        .await;

        let search = read_line(&mut server).await;
        assert!(
            search.contains("UID SEARCH ALL"),
            "CONDSTORE detects expunges by UID-list diff, got {search}"
        );
        respond(
            &mut server,
            &format!(
                "* SEARCH 2 3 4\r\n{} OK UID SEARCH done\r\n",
                tag_of(&search)
            ),
        )
        .await;
        server
    });

    let events = collect_changes(
        account,
        cursor_for(&FolderCursor::QResync {
            uidvalidity: 7,
            modseq: 50,
            known_uids: CompactUidSet::from_uids([1, 2, 3]),
            known_uids_complete: true,
        }),
    )
    .await;

    let warnings = warnings_of(&events);
    assert_eq!(warnings.len(), 1, "one downgrade warning: {warnings:?}");
    assert_eq!(warnings[0].kind, WarningKind::StrategyDowngraded);
    assert_eq!(
        warnings[0]
            .protocol_detail
            .as_ref()
            .map(DiagnosticText::as_str),
        Some(downgrade_detail(SyncStrategy::QResync, SyncStrategy::Condstore).as_str())
    );
    assert_eq!(
        change_labels(&events),
        vec![
            (id(7, 2), "updated"),
            (id(7, 4), "added"),
            (id(7, 1), "removed"),
        ]
    );
    match done_cursor(&events) {
        FolderCursor::Condstore {
            uidvalidity,
            modseq,
            known_uids,
        } => {
            assert_eq!((uidvalidity, modseq), (7, 60));
            assert_eq!(known_uids.to_uids(), vec![2, 3, 4]);
        }
        other => panic!("expected a CONDSTORE checkpoint, got {other:?}"),
    }
    let _server = script.await.unwrap();
}

/// A Basic cursor on a server with neither extension: no CONDSTORE parameter,
/// no CHANGEDSINCE, both lanes derived from the UID-list diff, and the
/// checkpoint carrying UIDNEXT rather than a mod-sequence.
#[tokio::test]
async fn basic_cursor_derives_both_lanes_from_a_uid_search_diff() {
    let (conn, mut server) = driver_pair(&preauth_greeting("IMAP4rev1")).await;
    let account = scripted_sync_account(conn, 1, false, None, inbox_registry());

    let script = tokio::spawn(async move {
        let select = read_line(&mut server).await;
        assert!(
            select.contains("EXAMINE")
                && !select.contains("CONDSTORE")
                && !select.contains("QRESYNC"),
            "a server advertising neither extension gets a bare EXAMINE, got {select}"
        );
        respond(
            &mut server,
            &format!(
                "{SELECT_PREAMBLE}\
                 * 2 EXISTS\r\n\
                 * OK [UIDVALIDITY 3] ok\r\n\
                 * OK [UIDNEXT 11] ok\r\n\
                 {} OK [READ-ONLY] EXAMINE done\r\n",
                tag_of(&select)
            ),
        )
        .await;

        let search = read_line(&mut server).await;
        assert!(
            search.contains("UID SEARCH ALL") && !search.contains("CHANGEDSINCE"),
            "expected the plain UID SEARCH ALL diff, got {search}"
        );
        respond(
            &mut server,
            &format!("* SEARCH 2 5\r\n{} OK UID SEARCH done\r\n", tag_of(&search)),
        )
        .await;
        server
    });

    let events = collect_changes(
        account,
        cursor_for(&FolderCursor::Basic {
            uidvalidity: 3,
            uidnext: 10,
            known_uids: CompactUidSet::from_uids([1, 2]),
        }),
    )
    .await;

    assert!(
        warnings_of(&events).is_empty(),
        "{:?}",
        warnings_of(&events)
    );
    assert_eq!(
        change_labels(&events),
        vec![(id(3, 5), "added"), (id(3, 1), "removed")]
    );
    match done_cursor(&events) {
        FolderCursor::Basic {
            uidvalidity,
            uidnext,
            known_uids,
        } => {
            assert_eq!((uidvalidity, uidnext), (3, 11));
            assert_eq!(known_uids.to_uids(), vec![2, 5]);
        }
        other => panic!("expected a Basic checkpoint, got {other:?}"),
    }
    let _server = script.await.unwrap();
}

/// A mailbox that answers the CONDSTORE select with `[NOMODSEQ]` downgrades to
/// Basic on the connection it already holds - two commands total, no second
/// checkout - and checkpoints a Basic cursor so the next round does not ask
/// for mod-sequences again.
#[tokio::test]
async fn nomodseq_mailbox_downgrades_condstore_to_basic_on_the_selected_connection() {
    let (conn, mut server) = driver_pair(&preauth_greeting("IMAP4rev1 CONDSTORE")).await;
    let account = scripted_sync_account(conn, 1, false, None, inbox_registry());

    let script = tokio::spawn(async move {
        let select = read_line(&mut server).await;
        assert!(select.contains("EXAMINE"), "expected EXAMINE, got {select}");
        respond(
            &mut server,
            &format!(
                "{SELECT_PREAMBLE}\
                 * 2 EXISTS\r\n\
                 * OK [UIDVALIDITY 9] ok\r\n\
                 * OK [UIDNEXT 7] ok\r\n\
                 * OK [NOMODSEQ] no mod-sequences\r\n\
                 {} OK [READ-ONLY] EXAMINE done\r\n",
                tag_of(&select)
            ),
        )
        .await;

        let search = read_line(&mut server).await;
        assert!(
            search.contains("UID SEARCH ALL"),
            "the Basic fallback must reuse the already selected connection, got {search}"
        );
        respond(
            &mut server,
            &format!("* SEARCH 1 2\r\n{} OK UID SEARCH done\r\n", tag_of(&search)),
        )
        .await;
        server
    });

    let events = collect_changes(
        account,
        cursor_for(&FolderCursor::Condstore {
            uidvalidity: 9,
            modseq: 40,
            known_uids: CompactUidSet::from_uids([1, 2]),
        }),
    )
    .await;

    let warnings = warnings_of(&events);
    assert_eq!(
        warnings.len(),
        1,
        "expected one downgrade warning: {warnings:?}"
    );
    assert_eq!(warnings[0].kind, WarningKind::StrategyDowngraded);
    assert_eq!(
        warnings[0]
            .protocol_detail
            .as_ref()
            .map(DiagnosticText::as_str),
        Some(downgrade_detail(SyncStrategy::Condstore, SyncStrategy::Basic).as_str())
    );
    assert!(
        !events.iter().any(|e| matches!(e, SyncEvent::Batch(_))),
        "nothing moved, so the downgrade must not manufacture a batch"
    );
    match done_cursor(&events) {
        FolderCursor::Basic {
            uidvalidity,
            uidnext,
            known_uids,
        } => {
            assert_eq!((uidvalidity, uidnext), (9, 7));
            assert_eq!(known_uids.to_uids(), vec![1, 2]);
        }
        other => panic!("a NOMODSEQ mailbox must checkpoint Basic, got {other:?}"),
    }
    let _server = script.await.unwrap();
}

/// A UIDVALIDITY change is terminal for the scope, not a diff: the stream
/// stops after the SELECT with a `RestartScope` directive and publishes no
/// batch, because every id minted under the old epoch is now stale.
#[tokio::test]
async fn uidvalidity_change_terminates_the_stream_without_publishing_a_diff() {
    let (conn, mut server) = driver_pair(&preauth_greeting("IMAP4rev1 CONDSTORE")).await;
    let account = scripted_sync_account(conn, 1, false, None, inbox_registry());

    let script = tokio::spawn(async move {
        let select = read_line(&mut server).await;
        assert!(select.contains("EXAMINE"), "expected EXAMINE, got {select}");
        respond(
            &mut server,
            &format!(
                "{SELECT_PREAMBLE}\
                 * 1 EXISTS\r\n\
                 * OK [UIDVALIDITY 8] rebuilt\r\n\
                 * OK [UIDNEXT 2] ok\r\n\
                 * OK [HIGHESTMODSEQ 3] ok\r\n\
                 {} OK [READ-ONLY] EXAMINE done\r\n",
                tag_of(&select)
            ),
        )
        .await;
        server
    });

    let events = collect_changes(
        account,
        cursor_for(&FolderCursor::Condstore {
            uidvalidity: 7,
            modseq: 50,
            known_uids: CompactUidSet::from_uids([1]),
        }),
    )
    .await;

    let last = events.last().expect("terminal event");
    let SyncEvent::Terminated(error) = last else {
        panic!("a UIDVALIDITY change must terminate the stream, got {last:?}");
    };
    assert!(
        matches!(
            error.recovery(),
            bifrost_types::RecoveryClass::Engine(bifrost_types::EngineDirective::RestartScope(
                bifrost_types::CursorScope::Folder(_)
            ))
        ),
        "expected RestartScope, got {:?}",
        error.recovery()
    );
    assert!(
        !events.iter().any(|e| matches!(e, SyncEvent::Batch(_))),
        "no diff may be published across a UIDVALIDITY epoch break"
    );
    let _server = script.await.unwrap();
}

/// `describe_cursor` is the engine's scheduling hint, so it must agree with
/// what `changes_stream` will actually do. The load-bearing case is the
/// QRESYNC cursor on a session where QRESYNC was disabled: the run downgrades
/// to CONDSTORE, and the descriptor has to say Condstore/Medium rather than
/// promising the cheap QRESYNC round.
#[tokio::test]
async fn describe_cursor_reports_the_strategy_the_run_will_actually_use() {
    use bifrost_types::CostClass;

    let qresync_cursor = cursor_for(&FolderCursor::QResync {
        uidvalidity: 1,
        modseq: 1,
        known_uids: CompactUidSet::default(),
        known_uids_complete: true,
    });
    let condstore_cursor = cursor_for(&FolderCursor::Condstore {
        uidvalidity: 1,
        modseq: 1,
        known_uids: CompactUidSet::default(),
    });
    let basic_cursor = cursor_for(&FolderCursor::Basic {
        uidvalidity: 1,
        uidnext: 1,
        known_uids: CompactUidSet::default(),
    });

    let (conn, _server) =
        driver_pair(&preauth_greeting("IMAP4rev1 ENABLE CONDSTORE QRESYNC")).await;
    let enabled = scripted_sync_account(conn, 1, true, None, inbox_registry());
    let described = super::changes::describe_cursor(&enabled, &qresync_cursor);
    assert_eq!(described.strategy, SyncStrategy::QResync);
    assert_eq!(described.cost_class, CostClass::Cheap);

    let (conn, _server) = driver_pair(&preauth_greeting("IMAP4rev1 CONDSTORE")).await;
    let disabled = scripted_sync_account(conn, 1, false, None, inbox_registry());
    let described = super::changes::describe_cursor(&disabled, &qresync_cursor);
    assert_eq!(
        described.strategy,
        SyncStrategy::Condstore,
        "a QRESYNC cursor on a downgraded session must be described as CONDSTORE"
    );
    assert_eq!(described.cost_class, CostClass::Medium);

    let described = super::changes::describe_cursor(&disabled, &condstore_cursor);
    assert_eq!(described.strategy, SyncStrategy::Condstore);
    assert_eq!(described.cost_class, CostClass::Medium);

    let described = super::changes::describe_cursor(&disabled, &basic_cursor);
    assert_eq!(described.strategy, SyncStrategy::Basic);
    assert_eq!(described.cost_class, CostClass::Expensive);
}

/// EXISTS disagreeing with the derived live UID set is a server-consistency
/// signal, not a fatal: the round still checkpoints, but it carries a warning
/// naming both counts so the drift is visible to the engine.
#[tokio::test]
async fn uid_count_disagreeing_with_exists_warns_but_still_checkpoints() {
    let (conn, mut server) = driver_pair(&preauth_greeting("IMAP4rev1")).await;
    let account = scripted_sync_account(conn, 1, false, None, inbox_registry());

    let script = tokio::spawn(async move {
        let select = read_line(&mut server).await;
        respond(
            &mut server,
            &format!(
                "{SELECT_PREAMBLE}\
                 * 9 EXISTS\r\n\
                 * OK [UIDVALIDITY 3] ok\r\n\
                 * OK [UIDNEXT 4] ok\r\n\
                 {} OK [READ-ONLY] EXAMINE done\r\n",
                tag_of(&select)
            ),
        )
        .await;
        let search = read_line(&mut server).await;
        respond(
            &mut server,
            &format!("* SEARCH 1 2\r\n{} OK UID SEARCH done\r\n", tag_of(&search)),
        )
        .await;
        server
    });

    let events = collect_changes(
        account,
        cursor_for(&FolderCursor::Basic {
            uidvalidity: 3,
            uidnext: 3,
            known_uids: CompactUidSet::from_uids([1, 2]),
        }),
    )
    .await;

    let warnings = warnings_of(&events);
    assert_eq!(warnings.len(), 1, "{warnings:?}");
    assert_eq!(warnings[0].kind, WarningKind::Other);
    assert!(
        warnings[0].message.as_str().contains("SELECT EXISTS 9"),
        "the warning must carry both counts: {}",
        warnings[0].message.as_str()
    );
    assert!(matches!(done_cursor(&events), FolderCursor::Basic { .. }));
    let _server = script.await.unwrap();
}

/// `PageBoundary::Final` plus the checkpoint on the last batch, and the same
/// checkpoint repeated on `Done`. A consumer that commits on either one must
/// land on the identical cursor.
#[tokio::test]
async fn the_final_batch_and_done_carry_the_same_checkpoint() {
    let (conn, mut server) = driver_pair(&preauth_greeting("IMAP4rev1")).await;
    let account = scripted_sync_account(conn, 1, false, None, inbox_registry());

    let script = tokio::spawn(async move {
        let select = read_line(&mut server).await;
        respond(
            &mut server,
            &format!(
                "{SELECT_PREAMBLE}\
                 * 1 EXISTS\r\n\
                 * OK [UIDVALIDITY 3] ok\r\n\
                 * OK [UIDNEXT 6] ok\r\n\
                 {} OK [READ-ONLY] EXAMINE done\r\n",
                tag_of(&select)
            ),
        )
        .await;
        let search = read_line(&mut server).await;
        respond(
            &mut server,
            &format!("* SEARCH 5\r\n{} OK UID SEARCH done\r\n", tag_of(&search)),
        )
        .await;
        server
    });

    let events = collect_changes(
        account,
        cursor_for(&FolderCursor::Basic {
            uidvalidity: 3,
            uidnext: 5,
            known_uids: CompactUidSet::default(),
        }),
    )
    .await;

    let batch = events
        .iter()
        .find_map(|event| match event {
            SyncEvent::Batch(batch) => Some(batch),
            _ => None,
        })
        .expect("one batch");
    assert!(matches!(batch.page_boundary, PageBoundary::Final));
    let Some(Checkpoint::Change(batch_cursor)) = batch.checkpoint.as_ref() else {
        panic!("the final batch must carry the checkpoint");
    };
    let batch_cursor = super::decode_cursor(batch_cursor).expect("decodes");
    assert_eq!(
        batch_cursor,
        done_cursor(&events),
        "committing on the final batch and committing on Done must agree"
    );
    let _server = script.await.unwrap();
}

// ---------------------------------------------------------------------------
// Account-lifetime conformance: the trait-surface contracts a transcript test
// cannot express, because they are about what the account does WITHOUT
// speaking to a server - closing, refusing an unsupported lane, and holding a
// lifecycle stream open.
// ---------------------------------------------------------------------------

/// `Account::close` is idempotent and terminal.
///
/// The engine may close an account it already closed (shutdown racing a
/// failed reopen). The second call must be a no-op `Ok`, not a second
/// LOGOUT sweep, and after either call the pool must refuse every
/// checkout instead of dialing a replacement session that would outlive
/// the account handle.
#[tokio::test]
async fn close_is_idempotent_and_leaves_the_pool_refusing() {
    use bifrost_types::Account;

    let (conn, server) = driver_pair(&preauth_greeting("IMAP4rev1")).await;
    let account = scripted_account(conn, 1);
    // No responder for the LOGOUT close issues: drop the server end so it
    // fails fast rather than blocking on a read that never completes.
    drop(server);

    let first = tokio::time::timeout(Duration::from_secs(5), account.close())
        .await
        .expect("close must not block on an unresponsive peer");
    assert!(first.is_ok(), "close reports success: {first:?}");

    let second = tokio::time::timeout(Duration::from_secs(5), account.close())
        .await
        .expect("the second close must return immediately");
    assert!(second.is_ok(), "close is idempotent: {second:?}");

    assert!(
        account.shutdown.is_cancelled(),
        "close cancels the account shutdown token",
    );
    let Err(err) = account
        .checkout_for_folder(&crate::types::MailboxName::new("INBOX").unwrap())
        .await
    else {
        panic!("a closed account must not hand out connections");
    };
    assert!(matches!(err, crate::Error::Closed { .. }), "got {err:?}");
}

/// The two blob openers are statically unsupported (`blob_range` is
/// `BlobRangeSupport::No`, and nothing mints a `BlobHandle`), so both must
/// terminate the stream with `Unsupported(<their own operation>)` and then
/// end - never hang, and never mis-tag the operation, which is what the
/// engine's recovery mapping keys on.
#[tokio::test]
async fn the_blob_openers_terminate_unsupported_with_their_own_operation() {
    use bifrost_types::{
        Account, AccountErrorKind, AccountOperation, BlobCapabilities, BlobEncoding, BlobHandle,
        BlobId, ByteRange,
    };
    use futures::StreamExt;

    let (conn, _server) = driver_pair(&preauth_greeting("IMAP4rev1")).await;
    let account = scripted_account(conn, 1);

    let handle = || BlobHandle {
        id: BlobId("imapblob1:INBOX:1:2:1.2".to_owned()),
        size: None,
        content_type: None,
        digest: None,
        capabilities: BlobCapabilities {
            supports_range: false,
            supports_parallel: false,
            digest_available_pre_download: false,
            encoding: BlobEncoding::Raw8Bit,
        },
    };

    for (mut stream, op) in [
        (account.open_blob(handle()), AccountOperation::OpenBlob),
        (
            account.open_blob_range(
                handle(),
                ByteRange {
                    start: 0,
                    length: Some(16),
                },
            ),
            AccountOperation::OpenBlobRange,
        ),
    ] {
        let first = stream.next().await.expect("the stream emits a terminal");
        let SyncEvent::Terminated(err) = first else {
            panic!("expected Terminated, got {first:?}");
        };
        assert_eq!(err.kind(), &AccountErrorKind::Unsupported(op));
        assert_eq!(err.operation(), Some(op));
        assert!(
            matches!(stream.next().await, Some(SyncEvent::Done(None))),
            "an unsupported lane still closes with Done",
        );
        assert!(stream.next().await.is_none(), "Done is terminal");
    }
}

/// The engine's bandwidth knob writes the SHARED atomic every pooled dial
/// reads, and `None` means unlimited while `Some(0)` is clamped to 1 B/s.
///
/// The clamp is the load-bearing half: `0` in the token bucket is a divisor
/// that would stall the transport forever, and `None` and `Some(0)` are the
/// two spellings a caller is most likely to confuse.
#[tokio::test]
async fn the_bandwidth_cap_knob_writes_the_shared_atomic_and_clamps_zero() {
    use std::sync::atomic::Ordering;

    use bifrost_types::Account;

    let (conn, _server) = driver_pair(&preauth_greeting("IMAP4rev1")).await;
    let account = scripted_account(conn, 1);
    let shared = Arc::clone(&account.bandwidth_cap);

    account.set_bandwidth_cap(Some(4_096));
    assert_eq!(shared.load(Ordering::Acquire), 4_096);

    account.set_bandwidth_cap(None);
    assert_eq!(
        shared.load(Ordering::Acquire),
        u64::MAX,
        "None is unlimited, not zero",
    );

    account.set_bandwidth_cap(Some(0));
    assert_eq!(
        shared.load(Ordering::Acquire),
        1,
        "Some(0) clamps to 1 B/s rather than stalling the transport",
    );
}

/// IMAP emits no scope-lifecycle events, but the stream must stay OPEN:
/// the engine's lifecycle worker treats an early close as a fault. It ends
/// only when the account shuts down.
#[tokio::test]
async fn the_scope_lifecycle_stream_stays_open_until_shutdown() {
    use bifrost_types::Account;
    use futures::{FutureExt, StreamExt};

    let (conn, _server) = driver_pair(&preauth_greeting("IMAP4rev1")).await;
    let account = scripted_account(conn, 1);

    let mut stream = account.scope_lifecycle_stream();
    assert!(
        stream.next().now_or_never().is_none(),
        "the lifecycle stream must be pending, not closed, while the account lives",
    );

    account.shutdown.cancel();
    let end = tokio::time::timeout(Duration::from_secs(5), stream.next())
        .await
        .expect("shutdown must end the lifecycle stream");
    assert!(end.is_none(), "shutdown closes the stream, got {end:?}");
}
