#![allow(clippy::unwrap_used, clippy::expect_used)]

use tokio::io::AsyncWriteExt;

use super::*;
use crate::connection::TcpKeepalive;
use crate::types::response::UntaggedResponse;

// ===========================================================================
// buffer_may_contain_complete_response
//
// This is the framing heuristic every read goes through: a false negative
// stalls the read loop waiting for bytes that already arrived, a false
// positive hands truncated bytes to the nom parser.
// ===========================================================================

#[test]
fn framing_no_crlf_is_incomplete() {
    assert!(!buffer_may_contain_complete_response(b""));
    assert!(!buffer_may_contain_complete_response(b"* 3 EXIST"));
}

#[test]
fn framing_plain_untagged_is_complete() {
    assert!(buffer_may_contain_complete_response(b"* 3 EXISTS\r\n"));
    assert!(buffer_may_contain_complete_response(b"* 1 EXPUNGE\r\n"));
}

#[test]
fn framing_status_text_braces_are_not_literals() {
    // RFC 3501 Section 9: `resp-text` is 1*TEXT-CHAR, so `{42}` in an
    // untagged status line is text, not a literal marker.
    assert!(buffer_may_contain_complete_response(
        b"* OK [ALERT] quota limit {42}\r\n"
    ));
    assert!(buffer_may_contain_complete_response(
        b"* BYE closing {7}\r\n"
    ));
    assert!(buffer_may_contain_complete_response(
        b"* NO trouble {7}\r\n"
    ));
    assert!(buffer_may_contain_complete_response(
        b"* BAD syntax {7}\r\n"
    ));
}

#[test]
fn framing_status_keyword_is_case_insensitive() {
    // RFC 3501 Section 9: alphabetic characters are case-insensitive; a
    // lowercasing server must not push us onto the literal path.
    assert!(buffer_may_contain_complete_response(
        b"* ok [ALERT] limit {42}\r\n"
    ));
}

#[test]
fn framing_tagged_status_braces_are_not_literals() {
    assert!(buffer_may_contain_complete_response(
        b"A001 OK done {42}\r\n"
    ));
    assert!(buffer_may_contain_complete_response(
        b"A001 NO nope {42}\r\n"
    ));
    assert!(buffer_may_contain_complete_response(
        b"A001 BAD bad {42}\r\n"
    ));
}

#[test]
fn framing_tagged_status_without_resp_text() {
    assert!(buffer_may_contain_complete_response(b"A001 OK\r\n"));
    assert!(buffer_may_contain_complete_response(b"A001 NO\r\n"));
    assert!(buffer_may_contain_complete_response(b"A001 BAD\r\n"));
}

#[test]
fn framing_continuation_is_complete() {
    // RFC 3501 Section 7.5: continuation requests carry no literal.
    assert!(buffer_may_contain_complete_response(b"+ ready for {5}\r\n"));
    assert!(buffer_may_contain_complete_response(b"+ \r\n"));
}

#[test]
fn framing_fetch_literal_body_missing_is_incomplete() {
    assert!(!buffer_may_contain_complete_response(
        b"* 1 FETCH (BODY[] {5}\r\nAB"
    ));
}

#[test]
fn framing_fetch_literal_body_complete() {
    assert!(buffer_may_contain_complete_response(
        b"* 1 FETCH (BODY[] {5}\r\nHELLO)\r\n"
    ));
}

#[test]
fn framing_fetch_multiple_literals() {
    assert!(buffer_may_contain_complete_response(
        b"* 1 FETCH (BODY[HEADER] {5}\r\nHELLO BODY[TEXT] {5}\r\nWORLD)\r\n"
    ));
    assert!(!buffer_may_contain_complete_response(
        b"* 1 FETCH (BODY[HEADER] {5}\r\nHELLO BODY[TEXT] {5}\r\nWOR"
    ));
}

#[test]
fn framing_non_synchronizing_literal_marker_is_honored() {
    // RFC 7888: `{N+}` on the server side still declares N octets.
    assert!(buffer_may_contain_complete_response(
        b"* 1 FETCH (BODY[] {5+}\r\nHELLO)\r\n"
    ));
    assert!(!buffer_may_contain_complete_response(
        b"* 1 FETCH (BODY[] {5+}\r\nHEL"
    ));
}

/// DOCUMENTS A BUG, NOT AN ENDORSEMENT.
///
/// `try_parse_literal_marker` only requires that *some* `}` follow the
/// last `{` before the CRLF - it never checks that the `}` abuts the
/// CRLF. So a quoted string ending in `{digits}` (a mailbox name, a
/// Subject, a regex quantifier in an ENVELOPE) is read as a literal
/// declaration, `pos` jumps past the end of a response that is in fact
/// complete, and the reader blocks waiting for octets that will never
/// arrive until the next server write unblocks it.
///
///
#[test]
fn framing_quoted_brace_digits_misread_as_literal() {
    let buf = b"* LIST (\\HasNoChildren) \"/\" \"Order {12}\"\r\n";
    assert!(
        !buffer_may_contain_complete_response(buf),
        "current behavior: a complete LIST response is reported incomplete"
    );
}

#[test]
fn framing_brace_digits_not_before_crlf_still_completes_when_more_follows() {
    // Same shape as the bug above, but with a following tagged line the
    // skip lands inside it and the scan recovers. This is why the defect
    // usually presents as a stall only on the last buffered response.
    let buf = b"* LIST () \"/\" \"Order {2}\"\r\nA001 OK LIST completed\r\n";
    assert!(buffer_may_contain_complete_response(buf));
}

#[test]
fn framing_unparenthesized_untagged_without_space_prefix() {
    // Not `* ` and not tagged-status: the conservative literal path.
    assert!(buffer_may_contain_complete_response(b"garbage\r\n"));
}

// ---------------------------------------------------------------------------
// try_parse_literal_marker
// ---------------------------------------------------------------------------

#[test]
fn marker_parses_plain_and_plus_forms() {
    // `crlf_pos` is the index of the `\r`.
    assert_eq!(try_parse_literal_marker(b"* X {12}\r\n", 8), Some(12));
    assert_eq!(try_parse_literal_marker(b"* X {12+}\r\n", 9), Some(12));
}

#[test]
fn marker_rejects_non_digits() {
    assert_eq!(try_parse_literal_marker(b"* X {ab}\r\n", 8), None);
    assert_eq!(try_parse_literal_marker(b"* X {}\r\n", 6), None);
    assert_eq!(try_parse_literal_marker(b"* X nothing\r\n", 11), None);
}

#[test]
fn marker_uses_the_last_open_brace() {
    // rposition, so an earlier decorative brace does not shadow the real
    // marker.
    assert_eq!(try_parse_literal_marker(b"* X {a} {7}\r\n", 11), Some(7));
}

// ===========================================================================
// WireReader over an in-memory duplex
// ===========================================================================

fn memory_reader() -> (WireReader, tokio::io::DuplexStream) {
    let (client, server) = tokio::io::duplex(1 << 16);
    (WireReader::new(ImapStream::Memory(client)), server)
}

#[tokio::test]
async fn read_greeting_parses_ok_greeting() {
    let (mut reader, mut server) = memory_reader();
    server
        .write_all(b"* OK [CAPABILITY IMAP4rev1] ready\r\n")
        .await
        .unwrap();
    server.flush().await.unwrap();

    let resp = reader.read_greeting().await.unwrap();
    match resp {
        Response::Greeting(g) => {
            assert!(matches!(
                g.status,
                crate::types::response::GreetingStatus::Ok
            ));
        }
        other => panic!("expected greeting, got {other:?}"),
    }
    assert!(reader.buffer_is_empty());
}

#[tokio::test]
async fn read_one_reassembles_a_response_split_across_writes() {
    let (mut reader, mut server) = memory_reader();

    let feeder = tokio::spawn(async move {
        // Deliberately split mid-token so the CRLF heuristic has to hold
        // the partial line back.
        server.write_all(b"* 3 EXI").await.unwrap();
        server.flush().await.unwrap();
        tokio::task::yield_now().await;
        server.write_all(b"STS\r\n").await.unwrap();
        server.flush().await.unwrap();
        server
    });

    let resp = reader.read_one(false).await.unwrap();
    let _server = feeder.await.unwrap();

    match resp {
        Response::Untagged(u) => assert!(matches!(*u, UntaggedResponse::Exists(3))),
        other => panic!("expected untagged EXISTS, got {other:?}"),
    }
}

#[tokio::test]
async fn read_one_reassembles_a_literal_split_across_writes() {
    let (mut reader, mut server) = memory_reader();

    let feeder = tokio::spawn(async move {
        server
            .write_all(b"* 1 FETCH (UID 7 BODY[TEXT] {11}\r\nHELLO")
            .await
            .unwrap();
        server.flush().await.unwrap();
        tokio::task::yield_now().await;
        server.write_all(b" WORLD)\r\n").await.unwrap();
        server.flush().await.unwrap();
        server
    });

    let resp = reader.read_one(false).await.unwrap();
    let _server = feeder.await.unwrap();

    match resp {
        Response::Untagged(u) => match *u {
            UntaggedResponse::Fetch(fetch) => {
                assert_eq!(fetch.uid, Some(7));
                assert_eq!(fetch.body_sections.len(), 1);
                assert_eq!(
                    fetch.body_sections[0].data.as_deref(),
                    Some(&b"HELLO WORLD"[..])
                );
            }
            other => panic!("expected FETCH, got {other:?}"),
        },
        other => panic!("expected untagged, got {other:?}"),
    }
}

#[tokio::test]
async fn read_one_returns_two_responses_from_one_segment() {
    let (mut reader, mut server) = memory_reader();
    server
        .write_all(b"* 3 EXISTS\r\nA001 OK NOOP completed\r\n")
        .await
        .unwrap();
    server.flush().await.unwrap();

    let first = reader.read_one(false).await.unwrap();
    assert!(matches!(first, Response::Untagged(_)));
    assert!(!reader.buffer_is_empty(), "tagged line stays buffered");

    let second = reader.read_one(false).await.unwrap();
    match second {
        Response::Tagged(t) => assert_eq!(t.tag, "A001"),
        other => panic!("expected tagged, got {other:?}"),
    }
    assert!(reader.buffer_is_empty());
}

#[tokio::test]
async fn read_one_reports_eof_as_closed() {
    let (mut reader, server) = memory_reader();
    drop(server);
    match reader.read_one(false).await {
        Err(Error::Closed { .. }) => {}
        other => panic!("expected Closed on EOF, got {other:?}"),
    }
}

#[tokio::test]
async fn read_one_reports_a_hard_parse_error() {
    let (mut reader, mut server) = memory_reader();
    // Terminated with CRLF so the incomplete-data heuristic cannot
    // swallow it, but not a valid response.
    server.write_all(b"%%%\r\n").await.unwrap();
    server.flush().await.unwrap();
    match reader.read_one(false).await {
        Err(Error::Parse(msg)) => assert!(msg.contains("parse error"), "unexpected text: {msg}"),
        other => panic!("expected Parse, got {other:?}"),
    }
}

#[tokio::test]
async fn write_all_reaches_the_peer() {
    use tokio::io::AsyncReadExt;

    let (mut reader, mut server) = memory_reader();
    reader.write_all(b"A001 NOOP\r\n").await.unwrap();

    let mut buf = vec![0u8; 11];
    server.read_exact(&mut buf).await.unwrap();
    assert_eq!(&buf[..], b"A001 NOOP\r\n");
}

#[tokio::test]
async fn take_buffer_hands_over_unparsed_bytes() {
    // RFC 4978 Section 3: on COMPRESS the post-OK bytes already read are
    // compressed data that must survive the stream swap.
    let (mut reader, mut server) = memory_reader();
    server
        .write_all(b"A001 OK COMPRESS active\r\nLEFTOVER")
        .await
        .unwrap();
    server.flush().await.unwrap();

    let resp = reader.read_one(false).await.unwrap();
    assert!(matches!(resp, Response::Tagged(_)));
    assert_eq!(&reader.take_buffer()[..], b"LEFTOVER");
    assert!(reader.buffer_is_empty());
}

#[tokio::test]
async fn keepalive_and_peer_certificate_are_unavailable_on_memory_streams() {
    let (reader, _server) = memory_reader();
    assert!(reader.peer_certificate_der().is_none());
    let ka = TcpKeepalive::new(
        std::time::Duration::from_secs(1),
        std::time::Duration::from_secs(1),
    );
    assert!(reader.set_keepalive(&ka).is_err());
}
