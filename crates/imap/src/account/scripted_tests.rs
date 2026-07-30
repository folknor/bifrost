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
        folders: Arc::new(FolderRegistry::default()),
        qresync_enabled: false,
        qresync_negotiation_warning: None,
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
    use bifrost_types::{ItemOutcome, Projection, SyncEvent};

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
