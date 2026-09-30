//! Byte-level transcripts for the single-command literal-continuation wait.
//!
//! `LISTRIGHTS` with a non-ASCII identifier is the one ordinary command that
//! produces a synchronizing literal, and a capability set with no `LITERAL+`,
//! `LITERAL-` or `IMAP4rev2` keeps it synchronizing, so the driver stops after
//! the command line and waits for a `+`. See `pipeline_tests` for the same
//! window inside a pipelined batch.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::time::Duration;

use crate::connection::test_support::{
    driver_pair, preauth_greeting, read_exact, read_line, respond, tag_of,
};
use crate::error::Error;
use crate::types::AppendMessage;

const SYNC_LITERAL_CAPS: &str = "IMAP4rev1 ACL";
const LITERAL_IDENTIFIER: &str = "\u{fc}ser";

/// The command's own tagged `OK` before its `+` is the non-fatal
/// `ProtocolMissing`, the literal is never written, and the connection carries
/// the next command.
///
/// Reverting the own-tag `Ok` arm of `wait_for_continuation` to a fatal
/// `Protocol` fails the `is_alive` and error-kind assertions; writing the
/// literal body anyway fails the NOOP-line assertion, because the body would
/// arrive on the wire ahead of the NOOP.
#[tokio::test]
async fn an_own_tag_ok_before_the_continuation_is_non_fatal_and_the_connection_stays_usable() {
    let (conn, mut server) = driver_pair(&preauth_greeting(SYNC_LITERAL_CAPS)).await;

    let script = tokio::spawn(async move {
        let line = read_line(&mut server).await;
        assert!(
            line.ends_with("{5}\r\n"),
            "no synchronizing literal: {line:?}"
        );
        let tag = tag_of(&line).to_owned();
        respond(&mut server, &format!("{tag} OK not really\r\n")).await;

        let noop = read_line(&mut server).await;
        assert!(
            noop.contains("NOOP"),
            "the abandoned literal was written ahead of the next command: {noop:?}"
        );
        let tag = tag_of(&noop).to_owned();
        respond(&mut server, &format!("{tag} OK NOOP completed\r\n")).await;
        server
    });

    let err = conn
        .list_rights("INBOX", LITERAL_IDENTIFIER, Duration::from_secs(5))
        .await
        .expect_err("an OK without a continuation cannot carry the answer");
    assert!(matches!(err, Error::ProtocolMissing(_)), "got {err:?}");
    assert!(!err.is_connection_fatal());
    assert!(
        conn.is_alive(),
        "the framing is intact after the own-tag OK"
    );
    conn.noop(Duration::from_secs(5))
        .await
        .expect("the connection must carry the next command");
    let _server = script.await.unwrap();
}

/// A tagged response for a tag that is not the command's own cannot belong to
/// anything, since only one command is outstanding: `Protocol`, fatal, the
/// driver retires.
///
/// Reverting the foreign-tag arm of `wait_for_continuation` (folding it into
/// the own-tag arm, or skipping it as ignorable) fails this: the error would
/// not be `Protocol` and the channel would stay open.
#[tokio::test]
async fn a_foreign_tag_before_the_continuation_is_fatal() {
    let (conn, mut server) = driver_pair(&preauth_greeting(SYNC_LITERAL_CAPS)).await;
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();

    let script = tokio::spawn(async move {
        let line = read_line(&mut server).await;
        let own = tag_of(&line).to_owned();
        assert_ne!(own, "ZZZZZZZZ", "the stand-in foreign tag must differ");
        respond(&mut server, "ZZZZZZZZ OK who am I\r\n").await;
        let _ = release_rx.await;
    });

    let err = conn
        .list_rights("INBOX", LITERAL_IDENTIFIER, Duration::from_secs(5))
        .await
        .expect_err("a foreign tag is a desynchronization");
    assert!(matches!(err, Error::Protocol(_)), "got {err:?}");
    assert!(err.is_connection_fatal());
    assert!(
        !conn.is_alive(),
        "the driver must retire on a desynchronized wire"
    );
    let _ = release_tx.send(());
    script.await.unwrap();
}

/// A later message of a MULTIAPPEND refused with a tagged `NO` at its own
/// marker (RFC 3502) is that command's ordinary refusal: the connection stays
/// usable, message two's body is never written, and the next command's line is
/// the very next thing on the wire.
///
/// The wire stays in sync only because the last byte written before each wait is
/// the synchronizing marker, so the server has read everything the client sent
/// when it answers. Reverting `wait_for_continuation` to treat an own-tag `NO`
/// after a granted literal as fatal fails the `is_alive` and fatality
/// assertions. Making `send_chunked_segments` write past the marker before the
/// wait (for example the next segment's leading chunk) fails the NOOP-line
/// assertion, because those bytes would arrive ahead of the NOOP.
#[tokio::test]
async fn a_later_multiappend_message_refused_at_its_marker_leaves_the_wire_in_sync() {
    let (conn, mut server) = driver_pair(&preauth_greeting("IMAP4rev1 MULTIAPPEND")).await;

    let script = tokio::spawn(async move {
        let first = read_line(&mut server).await;
        assert!(first.ends_with("{3}\r\n"), "first marker: {first:?}");
        let tag = tag_of(&first).to_owned();
        respond(&mut server, "+ go\r\n").await;

        let body = read_exact(&mut server, 3).await;
        assert_eq!(&body[..], b"one");
        let second = read_line(&mut server).await;
        assert_eq!(second, " {3}\r\n", "second marker must end the write");
        respond(
            &mut server,
            &format!("{tag} NO [OVERQUOTA] second refused\r\n"),
        )
        .await;

        let noop = read_line(&mut server).await;
        assert!(
            noop.contains("NOOP"),
            "bytes were written past the refused marker: {noop:?}"
        );
        let tag = tag_of(&noop).to_owned();
        respond(&mut server, &format!("{tag} OK NOOP completed\r\n")).await;
        server
    });

    let messages = [AppendMessage::new("one"), AppendMessage::new("two")];
    let err = conn
        .multi_append("INBOX", &messages, Duration::from_secs(5))
        .await
        .expect_err("the second message was refused");
    assert!(matches!(err, Error::No { .. }), "got {err:?}");
    assert!(!err.is_connection_fatal());
    assert!(
        conn.is_alive(),
        "a refusal at a marker leaves the framing intact"
    );
    conn.noop(Duration::from_secs(5))
        .await
        .expect("the connection must carry the next command");
    let _server = script.await.unwrap();
}
