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

    // Both responses for command #1 land while command #2 is blocked waiting
    // for its `+`.
    respond(
        &mut server,
        &format!("* MYRIGHTS INBOX lrswipkxte\r\n{tag1} OK myrights done\r\n+ ready\r\n"),
    )
    .await;

    // The grant must actually release the literal body.
    let body = read_exact(&mut server, 5).await;
    assert_eq!(body, LITERAL_IDENTIFIER.as_bytes());
    let _trailer = read_line(&mut server).await;

    respond(&mut server, &format!("{tag2} OK listrights done\r\n")).await;

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

    respond(&mut server, &format!("{tag2} OK listrights done\r\n")).await;

    let results = task.await.unwrap().expect("the batch must not abort");
    let err = results[0]
        .as_ref()
        .expect_err("command #1's NO belongs to command #1");
    assert!(
        format!("{err}").contains("myrights refused"),
        "wrong error routed to command #1: {err}"
    );
    // Command #2's own outcome is its own business - this transcript sends it
    // no LISTRIGHTS data, so its consumer may well complain. What must never
    // happen is command #1's refusal surfacing as command #2's.
    if let Err(ref e2) = results[1] {
        assert!(
            !format!("{e2}").contains("myrights refused"),
            "command #1's failure was misattributed to command #2: {e2}"
        );
    }
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

    // Nothing further reached the wire: the abandoned literal body would
    // otherwise be parsed as a command.
    drop(server);
}

/// A tagged `OK` for the command whose literal has not been granted stays a
/// hard protocol error rather than becoming that command's result.
///
/// `NO`/`BAD` are an honest refusal of the prefix and can be finalized as the
/// command's ordinary result. An `OK` cannot: it claims successful execution
/// of a command the server has not received, and there is nothing that could
/// have succeeded (RFC 3501 Section 4.3).
#[tokio::test]
async fn a_tagged_ok_before_the_literal_continuation_is_a_protocol_error() {
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

    let _myrights = read_line(&mut server).await;
    let listrights = read_line(&mut server).await;
    let tag2 = tag_of(&listrights).to_owned();

    respond(&mut server, &format!("{tag2} OK not really\r\n")).await;

    let err = task
        .await
        .unwrap()
        .expect_err("an OK without a continuation is a protocol violation");
    assert!(
        format!("{err}").contains("unexpected OK before literal continuation"),
        "wrong error: {err}"
    );
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

/// The tag the generator will produce after `tag`.
///
/// `TagGenerator` emits `{prefix:08x}{counter:08x}` from a per-connection
/// random prefix, so the successor is derived rather than guessed at.
fn next_tag(tag: &str) -> String {
    let (prefix, counter) = tag.split_at(8);
    let n = u32::from_str_radix(counter, 16).expect("the counter is 8 hex digits");
    format!("{prefix}{:08x}", n + 1)
}
