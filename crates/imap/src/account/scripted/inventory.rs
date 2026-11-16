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
