use super::test_support::*;
use bifrost_types::{Account, InventoryEvent};
use futures::StreamExt;

async fn bounded_inventory(uidnext: Option<u32>, fail_last: bool) {
    let (conn, mut server) = driver_pair(&preauth_greeting("IMAP4rev1")).await;
    let account = scripted_sync_account(conn, 1, false, None, inbox_registry());
    let observed = account.clone();
    let (first_seen, mut first_page) = tokio::sync::oneshot::channel();
    let mut first_seen = Some(first_seen);
    let script = tokio::spawn(async move {
        let select = read_line(&mut server).await;
        assert!(select.contains("EXAMINE"), "{select}");
        let next = uidnext
            .map(|n| format!("* OK [UIDNEXT {n}] next\r\n"))
            .unwrap_or_default();
        respond(&mut server, &format!("* FLAGS (\\Seen)\r\n* 2 EXISTS\r\n* 0 RECENT\r\n* OK [UIDVALIDITY 5] epoch\r\n{next}{} OK selected\r\n", tag_of(&select))).await;
        if uidnext.is_none() {
            let search = read_line(&mut server).await;
            assert!(search.contains("UID SEARCH ALL"), "{search}");
            respond(
                &mut server,
                &format!("* SEARCH 1 65537\r\n{} OK found\r\n", tag_of(&search)),
            )
            .await;
        }
        for (range, uid) in [("2:65537", 65537), ("1", 1)] {
            let fetch = read_line(&mut server).await;
            assert!(fetch.contains(&format!("UID FETCH {range} ")), "{fetch}");
            assert!(
                observed
                    .folders
                    .get(&crate::types::MailboxName::new("INBOX").unwrap())
                    .unwrap()
                    .cursor()
                    .is_none()
            );
            if uid == 1 {
                (&mut first_page).await.unwrap();
            }
            if fail_last && uid == 1 {
                respond(&mut server, &format!("{} NO refused\r\n", tag_of(&fetch))).await;
            } else {
                respond(
                    &mut server,
                    &format!(
                        "* 1 FETCH (UID {uid} FLAGS (\\Seen))\r\n{} OK fetched\r\n",
                        tag_of(&fetch)
                    ),
                )
                .await;
            }
        }
        server
    });
    let folder = crate::types::MailboxName::new("INBOX").unwrap();
    let mut stream = account.inventory_stream(crate::account::folder_scope(&folder));
    let mut pages = 0;
    let mut done = false;
    let mut terminated = false;
    while let Some(event) = stream.next().await {
        match event {
            InventoryEvent::Batch(page) => {
                pages += 1;
                if pages == 1 {
                    assert_eq!(
                        page.items[0].id,
                        crate::account::encode_object_id(&folder, nz(5), nz(65537),)
                    );
                    assert!(page.checkpoint.offered().is_none());
                    assert!(account.folders.get(&folder).unwrap().cursor().is_none());
                    first_seen.take().unwrap().send(()).unwrap();
                } else {
                    assert!(page.checkpoint.offered().is_some());
                }
            }
            InventoryEvent::Done(completion) => {
                assert!(completion.checkpoint.offered().is_some());
                done = true;
            }
            InventoryEvent::Terminated(_) => terminated = true,
            _ => {}
        }
    }
    let _server = script.await.unwrap();
    assert_eq!(done, !fail_last);
    assert_eq!(terminated, fail_last);
    assert_eq!(pages, if fail_last { 1 } else { 2 });
    let cursor = account.folders.get(&folder).unwrap().cursor();
    assert_eq!(cursor.is_some(), !fail_last);
    if let Some(crate::account::FolderCursor::Basic { known_uids, .. }) = cursor {
        assert_eq!(known_uids, crate::account::UidSet::from_numbers([1, 65537]));
    }
}

#[tokio::test]
async fn inventory_descending_bands_delay_cursor_until_complete() {
    bounded_inventory(Some(65538), false).await;
}

#[tokio::test]
async fn inventory_descending_bands_search_for_missing_uidnext() {
    bounded_inventory(None, false).await;
}

#[tokio::test]
async fn inventory_descending_bands_failure_never_commits_partial_cursor() {
    bounded_inventory(Some(65538), true).await;
}

async fn partition_inventory(epoch: u32, sticky: bool, uidnext: bool, fail: bool) {
    use bifrost_types::{CoverageDomain, InventoryPartition, InventoryPartitioning, UidWindow};
    let (conn, mut server) = driver_pair(&preauth_greeting("IMAP4rev1")).await;
    let account = scripted_sync_account(conn, 1, false, None, inbox_registry());
    let folder = crate::types::MailboxName::new("INBOX").unwrap();
    let scope = crate::account::folder_scope(&folder);
    assert_eq!(
        account.inventory_partitioning(&scope),
        InventoryPartitioning::Full
    );
    let prior = crate::account::FolderCursor::Basic {
        uidvalidity: nz(5),
        uidnext: Some(nz(100)),
        known_uids: crate::account::UidSet::from_numbers([1, 99]),
    };
    account.folders.set_cursor(&folder, prior.clone());
    let partition = InventoryPartition::Uid(UidWindow::new(epoch, 90, 100).unwrap());
    let domain = CoverageDomain::for_partition(scope.clone(), &partition);
    let script = tokio::spawn(async move {
        let select = read_line(&mut server).await;
        assert!(select.contains("EXAMINE"), "{select}");
        let next = if uidnext {
            "* OK [UIDNEXT 101] next\r\n"
        } else {
            ""
        };
        let nonsticky = if sticky {
            ""
        } else {
            "* OK [UIDNOTSTICKY] temporary\r\n"
        };
        respond(&mut server, &format!("* FLAGS (\\Seen)\r\n* 2 EXISTS\r\n* 0 RECENT\r\n* OK [UIDVALIDITY 5] epoch\r\n{next}{nonsticky}{} OK selected\r\n", tag_of(&select))).await;
        if epoch == 5 && sticky {
            let fetch = read_line(&mut server).await;
            assert!(fetch.contains("UID FETCH 90:99 "), "{fetch}");
            let status = if fail { "NO refused" } else { "OK fetched" };
            respond(&mut server, &format!("* 1 FETCH (UID 1 FLAGS ())\r\n* 2 FETCH (UID 99 FLAGS (\\Seen))\r\n{} {status}\r\n", tag_of(&fetch))).await;
        }
        server
    });
    let mut stream = account.inventory_partition_stream(scope.clone(), partition);
    let mut done = false;
    let mut terminated = false;
    let mut ids = Vec::new();
    while let Some(event) = stream.next().await {
        match event {
            InventoryEvent::Batch(page) => {
                assert_eq!(page.coverage.domain, domain);
                assert!(page.checkpoint.offered().is_none());
                ids.extend(page.items.into_iter().map(|item| item.id));
            }
            InventoryEvent::Done(completion) => {
                assert_eq!(completion.coverage.domain, domain);
                assert!(completion.checkpoint.offered().is_none());
                done = true;
            }
            InventoryEvent::Terminated(error) => {
                if epoch != 5 || !sticky {
                    assert!(matches!(
                        error.kind(),
                        bifrost_types::AccountErrorKind::SyncState(_)
                    ));
                }
                terminated = true;
            }
            _ => {}
        }
    }
    let _server = script.await.unwrap();
    let success = epoch == 5 && sticky && !fail;
    assert_eq!(done, success);
    assert_eq!(terminated, !success);
    assert_eq!(account.folders.get(&folder).unwrap().cursor(), Some(prior));
    assert_eq!(ids.len(), usize::from(success));
    assert_eq!(
        account.inventory_partitioning(&scope),
        if sticky && uidnext {
            InventoryPartitioning::UidRange {
                max_uid: Some(100),
                uid_validity: 5,
            }
        } else {
            InventoryPartitioning::Full
        }
    );
}

#[tokio::test]
async fn inventory_partition_window_does_not_commit_cursor() {
    partition_inventory(5, true, true, false).await;
}

#[tokio::test]
async fn inventory_partition_epoch_mismatch_is_refused() {
    partition_inventory(6, true, true, false).await;
}

#[tokio::test]
async fn inventory_partition_nonsticky_is_refused_and_plans_full() {
    partition_inventory(5, false, true, false).await;
}

#[tokio::test]
async fn inventory_partition_missing_uidnext_plans_full() {
    partition_inventory(5, true, false, false).await;
}

#[tokio::test]
async fn inventory_partition_failed_fetch_has_no_completion() {
    partition_inventory(5, true, true, true).await;
}

#[tokio::test]
async fn inventory_partition_empty_windows_validate_epoch_without_fetch() {
    use bifrost_types::{InventoryPartition, UidWindow};
    for (from, to) in [(0, 0), (0, 1), (100, 100), (4294967296, 4294967296)] {
        let (conn, mut server) = driver_pair(&preauth_greeting("IMAP4rev1")).await;
        let account = scripted_sync_account(conn, 1, false, None, inbox_registry());
        let script = tokio::spawn(async move {
            let select = read_line(&mut server).await;
            respond(&mut server, &format!("* FLAGS ()\r\n* 0 EXISTS\r\n* 0 RECENT\r\n* OK [UIDVALIDITY 5] epoch\r\n* OK [UIDNEXT 1] next\r\n{} OK selected\r\n", tag_of(&select))).await;
            server
        });
        let scope = crate::account::folder_scope(&crate::types::MailboxName::new("INBOX").unwrap());
        let events = account
            .inventory_partition_stream(
                scope.clone(),
                InventoryPartition::Uid(UidWindow::new(5, from, to).unwrap()),
            )
            .collect::<Vec<_>>()
            .await;
        let _server = script.await.unwrap();
        assert_eq!(events.len(), 1);
        assert!(matches!(events[0], InventoryEvent::Done(_)));
        assert_eq!(
            account.inventory_partitioning(&scope),
            bifrost_types::InventoryPartitioning::UidRange {
                max_uid: Some(0),
                uid_validity: 5
            }
        );
    }
}
