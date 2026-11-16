use super::*;
use crate::connection::test_support::{driver_pair, preauth_greeting, read_line, respond, tag_of};
use std::time::Duration;

const T: Duration = Duration::from_secs(5);

const PDF_ROOT: &str = "(\"application\" \"pdf\" (\"name\" \"a.pdf\") NIL NIL \"base64\" 100 NIL \
                        (\"attachment\" (\"filename\" \"a.pdf\")) NIL NIL)";
const PLAIN: &str = "(\"text\" \"plain\" (\"charset\" \"us-ascii\") NIL NIL \"7bit\" 10 1)";
const MIXED_PDF: &str = "((\"text\" \"plain\" (\"charset\" \"us-ascii\") NIL NIL \"7bit\" 10 1)\
                         (\"application\" \"pdf\" (\"name\" \"report.pdf\") NIL NIL \"base64\" 100) \"mixed\")";

async fn selected() -> (ImapConnection, tokio::io::DuplexStream) {
    let (conn, mut server) = driver_pair(&preauth_greeting("IMAP4rev1")).await;
    let script = tokio::spawn(async move {
        let line = read_line(&mut server).await;
        let tag = tag_of(&line).to_owned();
        respond(
            &mut server,
            &format!(
                "* 4 EXISTS\r\n* 0 RECENT\r\n* FLAGS (\\Seen)\r\n* OK [UIDVALIDITY 1] UIDs valid\r\n{tag} OK [READ-WRITE] done\r\n"
            ),
        )
        .await;
        server
    });
    conn.select("INBOX", T).await.unwrap();
    (conn, script.await.unwrap())
}

#[tokio::test]
async fn negated_has_complements_within_the_surviving_universe() {
    let (conn, mut server) = selected().await;
    let script = tokio::spawn(async move {
        let line = read_line(&mut server).await;
        assert!(line.contains("UID SEARCH ALL"), "{line}");
        let tag = tag_of(&line).to_owned();
        respond(
            &mut server,
            &format!("* SEARCH 1 2 3 4\r\n{tag} OK done\r\n"),
        )
        .await;
        let line = read_line(&mut server).await;
        assert!(line.contains("UID FETCH 1:4 (UID BODYSTRUCTURE)"), "{line}");
        let tag = tag_of(&line).to_owned();
        // UID 1 is first seen through an unsolicited FLAGS update, UID 4 was
        // expunged and is silently omitted, and UID 99 is outside the set.
        respond(
            &mut server,
            &format!(
                "* 1 FETCH (UID 1 FLAGS (\\Seen))\r\n\
                 * 1 FETCH (UID 1 BODYSTRUCTURE {PDF_ROOT})\r\n\
                 * 2 FETCH (UID 2 BODYSTRUCTURE {PLAIN})\r\n\
                 * 3 FETCH (UID 3 BODYSTRUCTURE {MIXED_PDF})\r\n\
                 * 9 FETCH (UID 99 FLAGS ())\r\n\
                 {tag} OK done\r\n"
            ),
        )
        .await;
        server
    });
    let has = FilterNode::Has("pdf".to_owned());
    let all = SearchCriteria::new().all();
    let not_has = FilterNode::Not(Box::new(has));
    let result = matching_uids(&conn, Surface::Messages, &not_has, &all, T)
        .await
        .unwrap();
    let _server = script.await.unwrap();
    assert_eq!(result.into_iter().collect::<Vec<_>>(), [2]);
}

#[tokio::test]
async fn an_answered_uid_without_bodystructure_fails_rather_than_reads_as_no_attachment() {
    let (conn, mut server) = selected().await;
    let script = tokio::spawn(async move {
        let line = read_line(&mut server).await;
        let tag = tag_of(&line).to_owned();
        respond(&mut server, &format!("* SEARCH 1\r\n{tag} OK done\r\n")).await;
        let line = read_line(&mut server).await;
        let tag = tag_of(&line).to_owned();
        respond(
            &mut server,
            &format!("* 1 FETCH (UID 1 FLAGS (\\Seen))\r\n{tag} OK done\r\n"),
        )
        .await;
        server
    });
    let not_has = FilterNode::Not(Box::new(FilterNode::Has(String::new())));
    let error = matching_uids(
        &conn,
        Surface::Messages,
        &not_has,
        &SearchCriteria::new().all(),
        T,
    )
    .await
    .expect_err("an incomplete observation is not a negative");
    let _server = script.await.unwrap();
    assert!(matches!(
        error,
        crate::Error::Provider(crate::error::ProviderFailure::Missing(_))
    ));
}

#[tokio::test]
async fn server_subtrees_are_searched_once_and_composed_with_has() {
    let (conn, mut server) = selected().await;
    let script = tokio::spawn(async move {
        let line = read_line(&mut server).await;
        assert!(line.contains("UID SEARCH ALL"), "{line}");
        let tag = tag_of(&line).to_owned();
        respond(&mut server, &format!("* SEARCH 1 2 3\r\n{tag} OK done\r\n")).await;
        let line = read_line(&mut server).await;
        let tag = tag_of(&line).to_owned();
        respond(
            &mut server,
            &format!(
                "* 1 FETCH (UID 1 BODYSTRUCTURE {PDF_ROOT})\r\n\
                 * 2 FETCH (UID 2 BODYSTRUCTURE {PLAIN})\r\n\
                 * 3 FETCH (UID 3 BODYSTRUCTURE {PLAIN})\r\n\
                 {tag} OK done\r\n"
            ),
        )
        .await;
        let line = read_line(&mut server).await;
        assert!(line.contains("UID SEARCH FROM \"a\""), "{line}");
        let tag = tag_of(&line).to_owned();
        respond(&mut server, &format!("* SEARCH 2 7\r\n{tag} OK done\r\n")).await;
        server
    });
    let from = FilterNode::Server(SearchCriteria::new().from("a").unwrap());
    // Has(application/pdf) holds for UID 1; From a without any attachment
    // holds for UID 2. UID 7 matched the server leaf but is outside the
    // universe, so it can never surface.
    let tree = FilterNode::Or(vec![
        FilterNode::Has("application/pdf".to_owned()),
        FilterNode::And(vec![
            from.clone(),
            FilterNode::Not(Box::new(FilterNode::Has(String::new()))),
        ]),
    ]);
    let result = matching_uids(
        &conn,
        Surface::Messages,
        &tree,
        &SearchCriteria::new().all(),
        T,
    )
    .await
    .unwrap();
    let _server = script.await.unwrap();
    assert_eq!(result.into_iter().collect::<Vec<_>>(), [1, 2]);
}
