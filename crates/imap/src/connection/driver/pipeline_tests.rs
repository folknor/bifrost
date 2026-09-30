//! Byte-level transcripts for the pipeline send phase.
//!
//! These all exercise one window: a pipelined batch in `Synchronizing`
//! literal mode, where the commands go out one at a time and the batch is
//! blocked mid-send waiting for a `+` continuation. Responses belonging to
//! EARLIER commands arrive in that window, and before the read-time router
//! landed the send phase had no way to reach their consumers.
//!
//! Getting a batch into that window is fiddly and the constraints are worth
//! stating, because a transcript that misses any of them exercises nothing:
//!
//! * the greeting must advertise neither `LITERAL+` nor `LITERAL-` nor
//!   `IMAP4rev2`, or the literals are non-synchronizing and the whole batch
//!   goes out in one write with no wait to interrupt;
//! * the two commands must have DISTINCT `CommandKind`s, or
//!   `group_into_sub_batches` splits them into sequential sub-batches and
//!   there is never more than one tag outstanding;
//! * the SECOND command must carry a synchronizing literal. Most pipelinable
//!   commands cannot produce one: mailbox names go through mUTF-7 and come
//!   out ASCII-quotable. `LISTRIGHTS` can, because its identifier is passed
//!   through raw, so a non-ASCII identifier forces a literal.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use crate::connection::test_support::{
    driver_pair, preauth_greeting, read_exact, read_line, respond, tag_of,
};
use crate::types::validated::MailboxName;

/// A capability set with no literal extension and no `IMAP4rev2`, so
/// `build_encode_options` selects `LiteralMode::Synchronizing`.
const SYNC_LITERAL_CAPS: &str = "IMAP4rev1 ACL";

/// The identifier that forces the literal: 5 UTF-8 bytes, not ASCII.
const LITERAL_IDENTIFIER: &str = "\u{fc}ser";

/// An untagged response solicited by an EARLIER pipelined command, arriving
/// while a later command waits for its literal continuation, reaches the
/// consumer that asked for it.
///
/// This is the silent half of the defect. The send phase used to answer
/// untagged responses through the consumerless arm, which downgrades them to
/// an anonymous typed event. The batch then COMPLETED, so the caller got a
/// short result and no error at all - strictly worse than the tagged half,
/// which at least failed loudly.
///
/// The assertion is on the PAYLOAD, deliberately. `is_ok()` passed against
/// the bug: `MyRightsConsumer` finalizes happily on a tagged OK having
/// received no data at all.
#[tokio::test]
async fn an_earlier_commands_untagged_response_reaches_its_consumer_mid_literal() {
    let (conn, mut server) = driver_pair(&preauth_greeting(SYNC_LITERAL_CAPS)).await;

    let task = tokio::spawn(async move {
        conn.pipeline()
            .my_rights(MailboxName::new("INBOX").unwrap())
            .list_rights(
                MailboxName::new("INBOX").unwrap(),
                LITERAL_IDENTIFIER.to_owned(),
            )
            .execute_dynamic()
            .await
    });

    let myrights = read_line(&mut server).await;
    let tag1 = tag_of(&myrights).to_owned();
    let listrights = read_line(&mut server).await;
    let tag2 = tag_of(&listrights).to_owned();
    assert!(
        listrights.ends_with("{5}\r\n"),
        "LISTRIGHTS did not produce a synchronizing literal: {listrights:?}"
    );

    // ORDERING IS THE WHOLE TEST. Command #1's untagged data lands inside the
    // continuation wait, but its TAGGED completion must NOT - it comes after
    // the `+`, once the wait is over.
    //
    // Sending the tagged response inside the wait as well would make the old
    // code fail loudly on the tagged path (it answered any tagged response with
    // a protocol error), which is the OTHER half of this defect and already has
    // its own test. Then this transcript would pass for the wrong reason and
    // the silent half - batch completes, caller gets a short result, no error
    // at all - would stay unpinned. A previous version of this test made
    // exactly that mistake.
    respond(&mut server, "* MYRIGHTS INBOX lrswipkxte\r\n+ ready\r\n").await;

    // The grant must actually release the literal body.
    let body = read_exact(&mut server, 5).await;
    assert_eq!(body, LITERAL_IDENTIFIER.as_bytes());
    let _trailer = read_line(&mut server).await;

    // Both completions now, in the step 4 response loop.
    respond(
        &mut server,
        &format!("{tag1} OK myrights done\r\n{tag2} OK listrights done\r\n"),
    )
    .await;

    let results = task.await.unwrap().unwrap();
    let rights = results[0]
        .as_ref()
        .expect("command #1 must not fail")
        .downcast_ref::<String>()
        .expect("MYRIGHTS output is a String");
    assert_eq!(rights, "lrswipkxte");
}

/// An earlier command's tagged `NO` arriving mid-literal is that command's
/// own result, and the batch survives it.
///
/// Before the router, any tagged response ended the wait and became the error
/// of the command being WRITTEN: command #1's `NO` was reported as a failure
/// of command #2, command #1's real result was destroyed, and the whole batch
/// aborted on the `?` at the send site before the response loop ever ran.
#[tokio::test]
async fn an_earlier_commands_tagged_no_lands_in_its_own_result_slot() {
    let (conn, mut server) = driver_pair(&preauth_greeting(SYNC_LITERAL_CAPS)).await;

    let task = tokio::spawn(async move {
        conn.pipeline()
            .my_rights(MailboxName::new("INBOX").unwrap())
            .list_rights(
                MailboxName::new("INBOX").unwrap(),
                LITERAL_IDENTIFIER.to_owned(),
            )
            .execute_dynamic()
            .await
    });

    let myrights = read_line(&mut server).await;
    let tag1 = tag_of(&myrights).to_owned();
    let listrights = read_line(&mut server).await;
    let tag2 = tag_of(&listrights).to_owned();

    respond(
        &mut server,
        &format!("{tag1} NO myrights refused\r\n+ ready\r\n"),
    )
    .await;

    let body = read_exact(&mut server, 5).await;
    assert_eq!(body, LITERAL_IDENTIFIER.as_bytes());
    let _trailer = read_line(&mut server).await;

    // Command #2 gets its own LISTRIGHTS data (the identifier as a literal,
    // since it is not ASCII) and its own OK, so its slot has a definite right
    // answer: the parsed rights, not merely "some result other than #1's".
    respond(
        &mut server,
        &format!(
            "* LISTRIGHTS INBOX {{5}}\r\n{LITERAL_IDENTIFIER} lr x\r\n\
             {tag2} OK listrights done\r\n"
        ),
    )
    .await;

    let results = task.await.unwrap().expect("the batch must not abort");
    let err = results[0]
        .as_ref()
        .expect_err("command #1's NO belongs to command #1");
    assert!(
        format!("{err}").contains("myrights refused"),
        "wrong error routed to command #1: {err}"
    );
    let rights = results[1]
        .as_ref()
        .expect("command #1's refusal must not surface as command #2's")
        .downcast_ref::<crate::types::ListRightsResponse>()
        .expect("LISTRIGHTS output is a ListRightsResponse");
    assert_eq!(rights.required, "lr");
    assert_eq!(rights.optional, vec!["x".to_owned()]);
}

/// A `NO` answering the command whose literal is being negotiated is that
/// command's ordinary per-command result, the rest of its bytes are never
/// written, and the batch still returns every other command's result.
///
/// The server has refused the literal, so it is no longer parsing this
/// command (RFC 3501 Section 4.3); writing the body would have it read as a
/// new command line.
#[tokio::test]
async fn the_sending_commands_own_rejection_is_its_result_not_a_batch_error() {
    let (conn, mut server) = driver_pair(&preauth_greeting(SYNC_LITERAL_CAPS)).await;

    let task = tokio::spawn(async move {
        conn.pipeline()
            .my_rights(MailboxName::new("INBOX").unwrap())
            .list_rights(
                MailboxName::new("INBOX").unwrap(),
                LITERAL_IDENTIFIER.to_owned(),
            )
            .execute_dynamic()
            .await
    });

    let myrights = read_line(&mut server).await;
    let tag1 = tag_of(&myrights).to_owned();
    let listrights = read_line(&mut server).await;
    let tag2 = tag_of(&listrights).to_owned();

    // Refuse the literal, then answer command #1. Note the ORDER: command #1
    // is still outstanding when its sibling is rejected, so the batch has to
    // go back to the response loop rather than give up.
    respond(
        &mut server,
        &format!(
            "{tag2} BAD literal refused\r\n* MYRIGHTS INBOX lr\r\n{tag1} OK myrights done\r\n"
        ),
    )
    .await;

    let results = task.await.unwrap().expect("the batch must not abort");
    let rights = results[0]
        .as_ref()
        .expect("command #1 still completes normally")
        .downcast_ref::<String>()
        .unwrap();
    assert_eq!(rights, "lr");
    let err = results[1]
        .as_ref()
        .expect_err("the rejected command's BAD is its own result");
    assert!(
        format!("{err}").contains("literal refused"),
        "wrong error routed to the rejected command: {err}"
    );

    // The abandoned remainder must NOT have reached the wire - the server has
    // ended this command's parsing state, so those bytes would be read as a new
    // command line (RFC 3501 Section 4.3). This is the rule the whole
    // `Rejected` path exists to honour and it needs a real assertion: simply
    // dropping the server end verifies nothing, because a client that DID
    // write the body would pass that just as happily.
    let mut tail = Vec::new();
    let _ = tokio::time::timeout(
        std::time::Duration::from_millis(100),
        tokio::io::AsyncReadExt::read_to_end(&mut server, &mut tail),
    )
    .await;
    // Anything at all is not the assertion: dropping the connection at the end
    // of the test sends a graceful LOGOUT, which is ordinary teardown traffic
    // and would make a bare "no more bytes" check fail for the wrong reason.
    // The assertion is that the ABANDONED BYTES specifically never went out.
    assert!(
        !tail
            .windows(LITERAL_IDENTIFIER.len())
            .any(|w| w == LITERAL_IDENTIFIER.as_bytes()),
        "the refused literal's body reached the wire and would be parsed as a \
         command: {:?}",
        String::from_utf8_lossy(&tail)
    );
}

/// A tagged `OK` for the command whose literal has not been granted is that
/// command's own non-fatal `ProtocolMissing`, exactly as in the single-command
/// wait, and the batch carries on.
///
/// The wire is still in sync: the send phase is sequential while a literal is
/// synchronizing, so nothing of a later command has been written when the
/// early `OK` arrives, and the abandoned remainder is never written. The proof
/// is the third command: its line must be the very next thing on the wire
/// (a written literal body would arrive first and be read as a command line).
/// The `OK` is NOT finalized through the consumer, which would report success
/// for a command that never ran.
#[tokio::test]
async fn a_tagged_ok_before_the_literal_continuation_is_that_commands_missing_result() {
    let (conn, mut server) = driver_pair(&preauth_greeting(SYNC_LITERAL_CAPS)).await;

    let task = tokio::spawn(async move {
        conn.pipeline()
            .my_rights(MailboxName::new("INBOX").unwrap())
            .list_rights(
                MailboxName::new("INBOX").unwrap(),
                LITERAL_IDENTIFIER.to_owned(),
            )
            .get_acl(MailboxName::new("INBOX").unwrap())
            .execute_dynamic()
            .await
    });

    let myrights = read_line(&mut server).await;
    let tag1 = tag_of(&myrights).to_owned();
    let listrights = read_line(&mut server).await;
    let tag2 = tag_of(&listrights).to_owned();

    respond(&mut server, &format!("{tag2} OK not really\r\n")).await;

    let getacl = read_line(&mut server).await;
    assert!(
        getacl.contains("GETACL"),
        "the abandoned literal body reached the wire ahead of the next command: {getacl:?}"
    );
    let tag3 = tag_of(&getacl).to_owned();
    respond(
        &mut server,
        &format!("* MYRIGHTS INBOX lr\r\n{tag1} OK myrights done\r\n{tag3} OK getacl done\r\n"),
    )
    .await;

    let results = task.await.unwrap().expect("the batch must not abort");
    assert_eq!(results.len(), 3);
    let err = results[1]
        .as_ref()
        .expect_err("an OK before the continuation is not a success");
    assert!(
        matches!(err, crate::error::Error::ProtocolMissing(_)),
        "wrong error: {err:?}"
    );
    assert!(!err.is_connection_fatal());
    let rights = results[0]
        .as_ref()
        .expect("command #1 still completes normally")
        .downcast_ref::<String>()
        .unwrap();
    assert_eq!(rights, "lr");
}

/// A tagged response for a command the server has not been sent yet is a
/// protocol error, not a parked completion.
///
/// This is the counterpart of the unknown-tag rejection the step 4 loop
/// already performs. It matters because the alternative is a HANG: the send
/// phase would keep waiting for a continuation the server has already decided
/// not to grant.
#[tokio::test]
async fn a_completion_for_an_unsent_command_is_a_protocol_error() {
    let (conn, mut server) = driver_pair(&preauth_greeting(SYNC_LITERAL_CAPS)).await;

    let task = tokio::spawn(async move {
        conn.pipeline()
            .my_rights(MailboxName::new("INBOX").unwrap())
            .list_rights(
                MailboxName::new("INBOX").unwrap(),
                LITERAL_IDENTIFIER.to_owned(),
            )
            .get_acl(MailboxName::new("INBOX").unwrap())
            .execute_dynamic()
            .await
    });

    let _myrights = read_line(&mut server).await;
    let listrights = read_line(&mut server).await;
    let tag2 = tag_of(&listrights).to_owned();
    // Command #3's tag is command #2's successor; it has not been written.
    let unsent_tag = next_tag(&tag2);

    respond(&mut server, &format!("{unsent_tag} OK impossible\r\n")).await;

    let err = task
        .await
        .unwrap()
        .expect_err("the server cannot complete a command it has not received");
    assert!(
        format!("{err}").contains("unsent pipelined command"),
        "wrong error: {err}"
    );
}

/// A tag-correlated ESEARCH for the SECOND pipelined search, arriving while
/// the first search is still the head, reaches the command its tag names.
///
/// Head routing used to hand it to the first search's consumer, which
/// buffers a foreign-tagged ESEARCH and surrenders it as an event, so the
/// second search finalized on its tagged OK with no ESEARCH at all and failed
/// with "SEARCH RETURN OK but no ESEARCH response" - for a server that did
/// nothing but interleave two pipelined searches, which is exactly what the
/// search-correlator exists to allow (RFC 4466, RFC 4731 Section 3.1).
///
/// The two searches have distinct `CommandKind`s (`Search`, `SearchReturn`),
/// so `group_into_sub_batches` keeps them in one batch with both tags
/// outstanding. No literal is involved; this is the step 4 response loop.
#[tokio::test]
async fn a_tag_correlated_esearch_reaches_the_search_its_tag_names() {
    let (conn, mut server) = driver_pair(&preauth_greeting("IMAP4rev1 ESEARCH")).await;

    let task = tokio::spawn(async move {
        conn.pipeline()
            .uid_search("ALL".to_owned())
            .uid_search_return("ALL".to_owned(), vec!["ALL".to_owned()])
            .execute_dynamic()
            .await
    });

    let search = read_line(&mut server).await;
    let tag1 = tag_of(&search).to_owned();
    let search_return = read_line(&mut server).await;
    let tag2 = tag_of(&search_return).to_owned();

    // Command #2's ESEARCH comes FIRST, while command #1 is still the head.
    respond(
        &mut server,
        &format!(
            "* ESEARCH (TAG \"{tag2}\") UID ALL 7\r\n* SEARCH 3\r\n\
             {tag1} OK search done\r\n{tag2} OK search return done\r\n"
        ),
    )
    .await;

    let results = task.await.unwrap().expect("the batch must not abort");
    let first = results[0]
        .as_ref()
        .expect("command #1 completes normally")
        .downcast_ref::<crate::connection::SearchResult>()
        .expect("SEARCH output is a SearchResult");
    assert_eq!(first.ids, vec![3]);
    let second = results[1]
        .as_ref()
        .expect("command #2's correlated ESEARCH must reach command #2")
        .downcast_ref::<crate::types::EsearchResponse>()
        .expect("SEARCH RETURN output is an EsearchResponse");
    assert_eq!(second.tag.as_deref(), Some(tag2.as_str()));
    assert_eq!(second.all, vec![crate::types::UidRange::single(7)]);
}

/// A tag-correlated ESEARCH naming a pipelined search that has ALREADY
/// finalized is surplus data for a completed command and is dropped, not
/// published as an event.
///
/// The router used to let it fall through to head routing, where the still
/// active first search buffered it as a foreign-tagged ESEARCH and surrendered
/// it on its tagged OK, so it surfaced as an event the classifier says cannot
/// exist (ESEARCH is `OnlySolicited` inside a search, `Impossible` elsewhere).
/// The EXISTS beside it is the control: an ordinary `Either` response still
/// reaches the event queue through the same consumer, so an empty queue is
/// not what the assertion rests on.
#[tokio::test]
async fn a_surplus_esearch_for_a_finalized_search_is_dropped_not_published() {
    let (conn, mut server) = driver_pair(&preauth_greeting("IMAP4rev1 ESEARCH")).await;

    let task = tokio::spawn(async move {
        let results = conn
            .pipeline()
            .uid_search("ALL".to_owned())
            .uid_search_return("ALL".to_owned(), vec!["ALL".to_owned()])
            .execute_dynamic()
            .await;
        (conn, results)
    });

    let search = read_line(&mut server).await;
    let tag1 = tag_of(&search).to_owned();
    let search_return = read_line(&mut server).await;
    let tag2 = tag_of(&search_return).to_owned();

    // Command #2 completes first; a second ESEARCH naming it then arrives
    // while command #1 is still the head.
    respond(
        &mut server,
        &format!(
            "* ESEARCH (TAG \"{tag2}\") UID ALL 7\r\n{tag2} OK search return done\r\n\
             * ESEARCH (TAG \"{tag2}\") UID ALL 9\r\n* 5 EXISTS\r\n* SEARCH 3\r\n\
             {tag1} OK search done\r\n"
        ),
    )
    .await;

    let (conn, results) = task.await.unwrap();
    let results = results.expect("the batch must not abort");
    let first = results[0]
        .as_ref()
        .expect("command #1 completes normally")
        .downcast_ref::<crate::connection::SearchResult>()
        .expect("SEARCH output is a SearchResult");
    assert_eq!(first.ids, vec![3]);
    let second = results[1]
        .as_ref()
        .expect("command #2 completes normally")
        .downcast_ref::<crate::types::EsearchResponse>()
        .expect("SEARCH RETURN output is an EsearchResponse");
    assert_eq!(second.all, vec![crate::types::UidRange::single(7)]);

    let events = conn.drain_events().await;
    assert!(
        events
            .iter()
            .any(|e| matches!(e, crate::connection::typed_event::TypedEvent::Exists(5))),
        "the control EXISTS did not reach the event queue: {events:?}"
    );
    assert!(
        !events.iter().any(|e| matches!(
            e,
            crate::connection::typed_event::TypedEvent::Extension(u)
                if matches!(**u, crate::types::response::UntaggedResponse::Esearch(_))
        )),
        "a surplus ESEARCH for a finalized search was published: {events:?}"
    );
}

/// The tag the generator will produce after `tag`.
///
/// `TagGenerator` emits `{prefix:08x}{counter:08x}` from a per-connection
/// random prefix, so the successor is derived rather than guessed at.
fn next_tag(tag: &str) -> String {
    let (prefix, counter) = tag.split_at(8);
    let n = u32::from_str_radix(counter, 16).expect("the counter is 8 hex digits");
    format!("{prefix}{:08x}", n + 1)
}
