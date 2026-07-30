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

use bifrost_types::ContainerKind;

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

fn created_name() -> crate::types::MailboxName {
    crate::types::MailboxName::new("Archive".to_owned()).unwrap()
}
