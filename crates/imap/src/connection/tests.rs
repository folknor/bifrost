#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::similar_names,
    clippy::too_many_lines,
    clippy::items_after_statements,
    clippy::match_wildcard_for_single_variants,
    clippy::single_match_else,
    clippy::match_wild_err_arm,
    clippy::single_match,
    clippy::wildcard_imports
)]
use super::*;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;

// ===========================================================================
// Driver-panic supervision tests
//
// These tests verify invariant I7: a consumer that panics inside the
// driver task surfaces as `Error::DriverPanicked` on the caller side,
// with the panic message preserved. Post-panic commands fail fast with
// `DriverPanicked` or `DriverGone` rather than hanging.
//
// These live in the lib-test module (not tests/invariants.rs) because
// they need access to internal types: `Consumer`, `Command`,
// `submit_regular`.
// ===========================================================================

/// Create a test `ImapConnection` backed by the driver task with an
/// in-memory `DuplexStream` pair. Returns the connection and the
/// server-side stream for scripting responses.
///
/// Constructs the driver-based architecture: the `ImapConnection`
/// holds `cmd_tx`, `state_rx`, `events_rx`, and a `JoinHandle` for
/// the driver task.
async fn make_driver_test_pair() -> (ImapConnection, tokio::io::DuplexStream) {
    let (client, mut server) = tokio::io::duplex(65536);

    // Write greeting from server side.
    server
        .write_all(b"* OK [CAPABILITY IMAP4rev1] ready\r\n")
        .await
        .unwrap();
    server.flush().await.unwrap();

    // --- Pre-driver phase: mirror connect_with_tls_config's init ---
    let mut wire_reader = wire::WireReader::new(ImapStream::Memory(client));
    let mut proto_state = state::ProtocolState::new();
    let tag_gen = tag::TagGenerator::new();

    let (events_tx, events_rx) = tokio::sync::mpsc::channel::<typed_event::TypedEvent>(256);
    let event_sink = driver::event_sink::DriverEventSink::new(events_tx, None);

    // Read and process the greeting.
    let greeting = wire_reader.read_greeting().await.unwrap();
    if let Response::Greeting(ref g) = greeting {
        proto_state.apply_greeting(g).unwrap();
    }

    // Spawn the driver task.
    let (cmd_tx, cmd_rx) = tokio::sync::mpsc::channel(16);
    let (state_tx, state_rx) = tokio::sync::watch::channel(proto_state.snapshot());
    let handle = tokio::spawn(driver::driver_task(
        wire_reader,
        proto_state,
        tag_gen,
        cmd_rx,
        state_tx,
        event_sink,
    ));

    let conn = ImapConnection {
        cmd_tx,
        state_rx,
        events_rx: tokio::sync::Mutex::new(events_rx),
        driver_handle: tokio::sync::Mutex::new(Some(handle)),
        prebuilt_tag_counter: std::sync::atomic::AtomicU32::new(0),
        tls_active: std::sync::atomic::AtomicBool::new(false),
        host: "test".into(),
    };

    (conn, server)
}

/// Consumer that intentionally panics in `on_response`. Used by the
/// driver-panic supervision test to inject a panic into the driver task.
struct PanickingConsumer;

impl dispatch::Consumer for PanickingConsumer {
    type Output = ();

    fn on_response(
        &mut self,
        _resp: UntaggedResponse,
        _notify: NotifyFlags,
        _ctx: &dispatch::ConsumerContext,
    ) {
        panic!("intentional panic for test");
    }

    fn finalize(
        self: Box<Self>,
        _tagged: TaggedResponse,
        _ctx: &dispatch::ConsumerContext,
    ) -> Result<dispatch::Finalized<()>, Error> {
        Ok(dispatch::Finalized {
            output: (),
            reclassified_as_events: vec![],
        })
    }
}

// ---------------------------------------------------------------------------
// Driver-panic  -  consumer panic surfaces as DriverPanicked (I7)
// ---------------------------------------------------------------------------

/// The peer-certificate handle routes through the full driver path
/// (handle method -> `DriverCommand::PeerCertificate` -> driver-loop arm
/// -> `WireReader` -> `ImapStream::Memory`) and returns `None` over the
/// non-TLS in-memory transport. This pins the dispatch wiring: a missing
/// driver-loop arm would hang the oneshot and the test would time out.
#[tokio::test]
async fn peer_certificate_der_none_over_memory_stream() {
    let (conn, _server) = make_driver_test_pair().await;
    assert!(
        conn.peer_certificate_der().await.is_none(),
        "in-memory (non-TLS) transport must have no peer certificate DER"
    );
}

/// Invariant: a consumer that panics inside the driver task surfaces
/// as `Error::DriverPanicked` on the caller side. The panic message
/// is extracted from the `JoinError` and included in the error.
#[tokio::test]
async fn invariant_driver_panic_surfaces_as_error() {
    let (conn, mut server) = make_driver_test_pair().await;

    let server_task = tokio::spawn(async move {
        // Receive the CAPABILITY command from the driver.
        let mut buf = vec![0u8; 4096];
        let n = server.read(&mut buf).await.unwrap();
        let _cmd = String::from_utf8_lossy(&buf[..n]).to_string();

        // Send an untagged CAPABILITY response. This triggers
        // on_response on the PanickingConsumer, which panics.
        server
            .write_all(b"* CAPABILITY IMAP4rev1\r\n")
            .await
            .unwrap();
        // Also send the tagged OK in case the driver processes it
        // before the panic unwinds (it won't, but be safe).
        server
            .write_all(b"A001 OK CAPABILITY done\r\n")
            .await
            .unwrap();
        server.flush().await.unwrap();
        server
    });

    let result = conn
        .submit_regular(Command::Capability, PanickingConsumer)
        .await;
    let _server = server_task.await.unwrap();

    match result {
        Err(Error::DriverPanicked { message: msg, .. }) => {
            assert!(
                msg.contains("intentional panic for test"),
                "panic message not propagated: {msg}"
            );
        }
        other => panic!("expected DriverPanicked, got: {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// Driver-panic  -  subsequent commands fail fast (I7)
// ---------------------------------------------------------------------------

/// After the driver panics, subsequent commands must fail promptly
/// with `DriverPanicked` or `DriverGone`  -  not hang forever.
#[tokio::test]
async fn invariant_driver_panic_subsequent_commands_fail() {
    let (conn, mut server) = make_driver_test_pair().await;

    let server_task = tokio::spawn(async move {
        let mut buf = vec![0u8; 4096];
        let _ = server.read(&mut buf).await;
        server
            .write_all(b"* CAPABILITY IMAP4rev1\r\nA001 OK done\r\n")
            .await
            .unwrap();
        server.flush().await.unwrap();
        server
    });

    // Trigger the panic via PanickingConsumer.
    let _ = conn
        .submit_regular(Command::Capability, PanickingConsumer)
        .await;
    let _server = server_task.await.unwrap();

    // Post-panic: the next command must not hang.
    let result = tokio::time::timeout(
        Duration::from_millis(500),
        conn.submit_regular(Command::Noop, dispatch::TaggedOkConsumer::default()),
    )
    .await;

    assert!(
        result.is_ok(),
        "post-panic command must complete (not hang)"
    );
    let inner = result.unwrap();
    assert!(
        matches!(
            inner,
            Err(Error::DriverPanicked { .. } | Error::DriverGone { .. })
        ),
        "post-panic command should be DriverPanicked or DriverGone, got: {inner:?}"
    );
}

// ===========================================================================
// Pure helpers defined in `connection/mod.rs`
// ===========================================================================

// ---------------------------------------------------------------------------
// validate_tls_server_name
// ---------------------------------------------------------------------------

#[test]
fn tls_server_name_rejects_empty_and_whitespace_and_nul() {
    assert!(validate_tls_server_name("imap.example.test").is_ok());
    assert!(validate_tls_server_name("").is_err());
    assert!(validate_tls_server_name("imap example.test").is_err());
    assert!(validate_tls_server_name("imap.example.test\n").is_err());
    assert!(validate_tls_server_name("imap\0.example.test").is_err());
}

// ---------------------------------------------------------------------------
// filter_store_flags (RFC 3501 Section 2.3.2)
// ---------------------------------------------------------------------------

#[test]
fn store_flag_filter_drops_server_only_and_wildcard_flags() {
    let out = filter_store_flags(&[Flag::Recent, Flag::Seen, Flag::Wildcard, Flag::Deleted]);
    assert_eq!(out, vec![Flag::Seen, Flag::Deleted]);
}

// ---------------------------------------------------------------------------
// expand_uid_ranges (RFC 4731 Section 3.1)
// ---------------------------------------------------------------------------

#[test]
fn expand_uid_ranges_singles_and_ranges() {
    let (uids, truncated) = expand_uid_ranges(&[
        UidRange {
            start: 1,
            end: None,
        },
        UidRange {
            start: 4,
            end: Some(6),
        },
    ]);
    assert_eq!(uids, vec![1, 4, 5, 6]);
    assert!(!truncated);
}

#[test]
fn expand_uid_ranges_empty_input() {
    let (uids, truncated) = expand_uid_ranges(&[]);
    assert!(uids.is_empty());
    assert!(!truncated);
}

#[test]
fn expand_uid_ranges_star_sentinel_is_not_expanded() {
    // RFC 4731 Section 3.1: `*` is the highest UID in the mailbox, which
    // the ESEARCH response alone does not reveal.
    let (uids, truncated) = expand_uid_ranges(&[UidRange {
        start: 10,
        end: Some(u32::MAX),
    }]);
    assert_eq!(uids, vec![10]);
    assert!(truncated, "an unexpandable `*` range must be flagged");
}

#[test]
fn expand_uid_ranges_caps_at_one_million() {
    let (uids, truncated) = expand_uid_ranges(&[UidRange {
        start: 1,
        end: Some(3_000_000),
    }]);
    assert_eq!(uids.len(), 1_000_000);
    assert_eq!(uids[0], 1);
    assert_eq!(uids[999_999], 1_000_000);
    assert!(truncated);
}

#[test]
fn expand_uid_ranges_inverted_range_yields_nothing() {
    // Defensive: a non-conformant server sending `10:5` must not panic.
    let (uids, truncated) = expand_uid_ranges(&[UidRange {
        start: 10,
        end: Some(5),
    }]);
    assert!(uids.is_empty());
    assert!(!truncated);
}

// ---------------------------------------------------------------------------
// selected_mailbox_effective_responses (RFC 7162 Section 3.2.11)
// ---------------------------------------------------------------------------

fn ok_code(code: ResponseCode) -> UntaggedResponse {
    UntaggedResponse::Status {
        status: UntaggedStatus::Ok,
        code: Some(code),
        text: String::new(),
    }
}

#[test]
fn closed_response_code_splits_off_the_previous_mailbox() {
    let responses = vec![
        UntaggedResponse::Exists(1),
        ok_code(ResponseCode::Closed),
        UntaggedResponse::Exists(7),
    ];
    let effective = selected_mailbox_effective_responses(&responses);
    assert_eq!(effective.len(), 1);
    assert!(matches!(effective[0], UntaggedResponse::Exists(7)));
}

#[test]
fn without_closed_every_response_is_effective() {
    let responses = vec![UntaggedResponse::Exists(1), UntaggedResponse::Recent(0)];
    assert_eq!(selected_mailbox_effective_responses(&responses).len(), 2);
}

#[test]
fn only_the_last_closed_marker_counts() {
    let responses = vec![
        UntaggedResponse::Exists(1),
        ok_code(ResponseCode::Closed),
        UntaggedResponse::Exists(2),
        ok_code(ResponseCode::Closed),
        UntaggedResponse::Exists(3),
    ];
    let effective = selected_mailbox_effective_responses(&responses);
    assert_eq!(effective.len(), 1);
    assert!(matches!(effective[0], UntaggedResponse::Exists(3)));
}

// ---------------------------------------------------------------------------
// build_selected_mailbox (RFC 3501 Section 7 / RFC 7162 Section 3)
// ---------------------------------------------------------------------------

fn tagged(code: Option<ResponseCode>) -> TaggedResponse {
    TaggedResponse {
        tag: "A001".to_owned(),
        status: crate::types::response::StatusKind::Ok,
        code,
        text: "done".to_owned(),
    }
}

#[test]
fn selected_mailbox_collects_counts_flags_and_codes() {
    let untagged = vec![
        UntaggedResponse::Exists(17),
        UntaggedResponse::Recent(2),
        UntaggedResponse::Flags(vec![Flag::Seen, Flag::Draft]),
        ok_code(ResponseCode::UidValidity(42)),
        ok_code(ResponseCode::UidNext(100)),
        ok_code(ResponseCode::PermanentFlags(vec![Flag::Seen])),
        ok_code(ResponseCode::Unseen(3)),
        ok_code(ResponseCode::MailboxId("mb-1".to_owned())),
        ok_code(ResponseCode::UidNotSticky),
    ];
    let mailbox = build_selected_mailbox(&untagged, &tagged(None), false);

    assert_eq!(mailbox.exists, 17);
    assert_eq!(mailbox.recent, 2);
    assert_eq!(mailbox.flags, vec![Flag::Seen, Flag::Draft]);
    assert_eq!(mailbox.uid_validity, Some(42));
    assert_eq!(mailbox.uid_next, Some(100));
    assert_eq!(mailbox.permanent_flags, vec![Flag::Seen]);
    assert_eq!(mailbox.unseen, Some(3));
    assert_eq!(mailbox.mailbox_id.as_deref(), Some("mb-1"));
    assert!(mailbox.uid_not_sticky);
    assert!(!mailbox.read_only);
}

#[test]
fn selected_mailbox_reads_the_tagged_response_code_too() {
    // RFC 3501 Section 6.3.1: SELECT reports [READ-WRITE] / [UIDVALIDITY]
    // on the tagged line on some servers.
    let mailbox = build_selected_mailbox(&[], &tagged(Some(ResponseCode::UidValidity(9))), true);
    assert_eq!(mailbox.uid_validity, Some(9));
    assert!(mailbox.read_only);
}

#[test]
fn selected_mailbox_missing_uidvalidity_stays_none() {
    // Never fabricate the invalid sentinel 0 (RFC 3501 Section 9: nz-number).
    let mailbox = build_selected_mailbox(&[UntaggedResponse::Exists(1)], &tagged(None), false);
    assert_eq!(mailbox.uid_validity, None);
}

#[test]
fn selected_mailbox_highest_modseq_zero_is_treated_as_nomodseq() {
    // RFC 7162 Section 3.1.2.1: mod-sequence-value >= 1, so HIGHESTMODSEQ 0
    // is the server meaning [NOMODSEQ].
    let zero = build_selected_mailbox(
        &[ok_code(ResponseCode::HighestModSeq(0))],
        &tagged(None),
        false,
    );
    assert_eq!(zero.highest_mod_seq, None);
    assert!(zero.no_mod_seq);

    let real = build_selected_mailbox(
        &[ok_code(ResponseCode::HighestModSeq(77))],
        &tagged(None),
        false,
    );
    assert_eq!(real.highest_mod_seq, Some(77));
    assert!(!real.no_mod_seq);

    let none = build_selected_mailbox(&[ok_code(ResponseCode::NoModSeq)], &tagged(None), false);
    assert!(none.no_mod_seq);
}

#[test]
fn selected_mailbox_keeps_only_earlier_vanished() {
    // RFC 7162 Section 3.2.5.2: only VANISHED (EARLIER) belongs to the
    // initial QRESYNC sync; a bare VANISHED is an unsolicited expunge.
    let untagged = vec![
        UntaggedResponse::Vanished {
            earlier: true,
            uids: vec![UidRange {
                start: 1,
                end: Some(3),
            }],
        },
        UntaggedResponse::Vanished {
            earlier: false,
            uids: vec![UidRange {
                start: 9,
                end: None,
            }],
        },
    ];
    let mailbox = build_selected_mailbox(&untagged, &tagged(None), false);
    assert_eq!(mailbox.vanished.len(), 1);
    assert_eq!(mailbox.vanished[0].start, 1);
    assert_eq!(mailbox.vanished[0].end, Some(3));
}

#[test]
fn selected_mailbox_ignores_state_from_before_closed() {
    let untagged = vec![
        UntaggedResponse::Exists(99),
        ok_code(ResponseCode::UidValidity(1)),
        ok_code(ResponseCode::Closed),
        UntaggedResponse::Exists(4),
        ok_code(ResponseCode::UidValidity(2)),
    ];
    let mailbox = build_selected_mailbox(&untagged, &tagged(None), false);
    assert_eq!(mailbox.exists, 4);
    assert_eq!(mailbox.uid_validity, Some(2));
}

// ---------------------------------------------------------------------------
// NOTIFY classification helpers (RFC 5465 Section 5.4, RFC 5258 Section 3)
// ---------------------------------------------------------------------------

fn mailbox_info(attrs: Vec<MailboxAttribute>) -> MailboxInfo {
    MailboxInfo {
        name: MailboxName::new("INBOX").unwrap(),
        delimiter: Some('/'),
        attributes: attrs,
        old_name: None,
        child_info: Vec::new(),
    }
}

#[test]
fn oldname_always_marks_a_notify_list_event() {
    let mut info = mailbox_info(Vec::new());
    info.old_name = Some(MailboxName::new("Old").unwrap());
    assert!(is_notify_list_event(&info, true));
    assert!(
        is_notify_list_event(&info, false),
        "OLDNAME is a marker regardless of LIST-EXTENDED context"
    );
}

#[test]
fn nonexistent_and_noaccess_are_markers_only_outside_subscribed_listings() {
    for attr in [MailboxAttribute::NonExistent, MailboxAttribute::NoAccess] {
        let info = mailbox_info(vec![attr]);
        assert!(is_notify_list_event(&info, true));
        assert!(
            !is_notify_list_event(&info, false),
            "LIST-EXTENDED with SUBSCRIBED may legitimately return these"
        );
    }
}

#[test]
fn a_plain_list_response_is_not_a_notify_event() {
    let info = mailbox_info(vec![MailboxAttribute::HasNoChildren]);
    assert!(!is_notify_list_event(&info, true));
}

#[test]
fn selection_mismatch_detects_missing_subscribed() {
    let plain = mailbox_info(Vec::new());
    assert!(is_notify_selection_mismatch(&plain, &["SUBSCRIBED"]));

    let subscribed = mailbox_info(vec![MailboxAttribute::Subscribed]);
    assert!(!is_notify_selection_mismatch(&subscribed, &["SUBSCRIBED"]));
}

#[test]
fn selection_mismatch_accepts_childinfo_under_recursivematch() {
    // RFC 5258 Section 3.5: a parent with subscribed children is returned
    // without \Subscribed but with CHILDINFO.
    let mut info = mailbox_info(Vec::new());
    info.child_info = vec!["SUBSCRIBED".to_owned()];
    assert!(!is_notify_selection_mismatch(
        &info,
        &["SUBSCRIBED", "RECURSIVEMATCH"]
    ));
    assert!(
        is_notify_selection_mismatch(&info, &["SUBSCRIBED"]),
        "CHILDINFO only excuses the missing attribute under RECURSIVEMATCH"
    );
}

#[test]
fn selection_mismatch_checks_remote_and_special_use() {
    let plain = mailbox_info(Vec::new());
    assert!(is_notify_selection_mismatch(&plain, &["REMOTE"]));
    assert!(is_notify_selection_mismatch(&plain, &["SPECIAL-USE"]));

    let remote = mailbox_info(vec![MailboxAttribute::Remote]);
    assert!(!is_notify_selection_mismatch(&remote, &["remote"]));

    let archive = mailbox_info(vec![MailboxAttribute::Archive]);
    assert!(!is_notify_selection_mismatch(&archive, &["SPECIAL-USE"]));
}

#[test]
fn selection_mismatch_with_no_options_is_never_a_mismatch() {
    assert!(!is_notify_selection_mismatch(
        &mailbox_info(Vec::new()),
        &[]
    ));
}

// ---------------------------------------------------------------------------
// Pre-built command tags (RFC 3501 Section 2.2.1)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn prebuilt_tags_are_prefixed_and_monotonic() {
    let conn =
        crate::connection::test_support::detached(SessionState::Authenticated, Vec::new(), &[]);
    assert_eq!(conn.next_prebuilt_tag(), "P001");
    assert_eq!(conn.next_prebuilt_tag(), "P002");
    assert_eq!(conn.next_prebuilt_tag(), "P003");
}

#[tokio::test]
async fn driver_channel_liveness_is_independent_of_session_state() {
    let detached =
        crate::connection::test_support::detached(SessionState::Selected, Vec::new(), &[]);
    assert_eq!(detached.session_state(), SessionState::Selected);
    assert!(!detached.is_alive(), "the command receiver was dropped");

    let (live, _server) =
        crate::connection::test_support::driver_pair(&preauth_greeting("IMAP4rev1")).await;
    assert!(
        live.is_alive(),
        "the driver still owns the command receiver"
    );
}

// ===========================================================================
// Byte-level command transcripts over the in-memory duplex
// ===========================================================================

use crate::connection::test_support::{preauth_greeting, read_line, respond, tag_of};

#[tokio::test]
async fn select_round_trip_parses_the_untagged_block() {
    let (conn, mut server) =
        crate::connection::test_support::driver_pair(&preauth_greeting("IMAP4rev1 UIDPLUS")).await;

    let script = tokio::spawn(async move {
        let line = read_line(&mut server).await;
        assert!(line.contains("SELECT"), "unexpected command: {line}");
        assert!(line.contains("INBOX"), "unexpected command: {line}");
        let tag = tag_of(&line).to_owned();
        respond(
            &mut server,
            &format!(
                "* 3 EXISTS\r\n\
                 * 0 RECENT\r\n\
                 * FLAGS (\\Seen \\Deleted)\r\n\
                 * OK [UIDVALIDITY 4242] UIDs valid\r\n\
                 * OK [UIDNEXT 9] Predicted next UID\r\n\
                 {tag} OK [READ-WRITE] SELECT completed\r\n"
            ),
        )
        .await;
        server
    });

    let mailbox = conn.select("INBOX", Duration::from_secs(5)).await.unwrap();
    let _server = script.await.unwrap();

    assert_eq!(mailbox.exists, 3);
    assert_eq!(mailbox.uid_validity, Some(4242));
    assert_eq!(mailbox.uid_next, Some(9));
    assert_eq!(mailbox.flags, vec![Flag::Seen, Flag::Deleted]);
    assert!(!mailbox.read_only);
    assert_eq!(conn.session_state(), SessionState::Selected);
}

#[tokio::test]
async fn select_no_response_leaves_the_session_authenticated() {
    // RFC 3501 Section 6.3.1: a tagged NO deselects.
    let (conn, mut server) =
        crate::connection::test_support::driver_pair(&preauth_greeting("IMAP4rev1")).await;

    let script = tokio::spawn(async move {
        let line = read_line(&mut server).await;
        let tag = tag_of(&line).to_owned();
        respond(
            &mut server,
            &format!("{tag} NO [NONEXISTENT] no such mailbox\r\n"),
        )
        .await;
        server
    });

    let err = conn
        .select("Missing", Duration::from_secs(5))
        .await
        .expect_err("SELECT NO must surface as an error");
    let _server = script.await.unwrap();

    assert!(matches!(err, Error::No { .. }), "got {err:?}");
    assert_eq!(conn.session_state(), SessionState::Authenticated);
    assert!(conn.is_alive(), "a tagged NO leaves the wire reusable");
}

#[tokio::test]
async fn uid_fetch_round_trip_with_a_literal_body() {
    let (conn, mut server) =
        crate::connection::test_support::driver_pair(&preauth_greeting("IMAP4rev1")).await;

    // Get into Selected state first.
    let select_script = tokio::spawn(async move {
        let line = read_line(&mut server).await;
        let tag = tag_of(&line).to_owned();
        respond(
            &mut server,
            &format!(
                "* 1 EXISTS\r\n\
                 * 0 RECENT\r\n\
                 * FLAGS (\\Seen)\r\n\
                 * OK [UIDVALIDITY 1] UIDs valid\r\n\
                 {tag} OK [READ-WRITE] SELECT completed\r\n"
            ),
        )
        .await;
        server
    });
    conn.select("INBOX", Duration::from_secs(5)).await.unwrap();
    let mut server = select_script.await.unwrap();

    let fetch_script = tokio::spawn(async move {
        let line = read_line(&mut server).await;
        assert!(line.contains("UID FETCH"), "unexpected command: {line}");
        let tag = tag_of(&line).to_owned();
        respond(
            &mut server,
            &format!(
                "* 1 FETCH (UID 7 FLAGS (\\Seen) BODY[TEXT] {{11}}\r\nHELLO WORLD)\r\n\
                 {tag} OK UID FETCH completed\r\n"
            ),
        )
        .await;
        server
    });

    let set = SequenceSet::new("7").unwrap();
    let items = [
        FetchAttr::Uid,
        FetchAttr::Flags,
        FetchAttr::BodySection {
            peek: true,
            section: Some("TEXT".to_owned()),
            partial: None,
        },
    ];
    let fetched = conn
        .uid_fetch(&set, &items, Duration::from_secs(5))
        .await
        .unwrap();
    let _server = fetch_script.await.unwrap();

    assert_eq!(fetched.len(), 1);
    assert_eq!(fetched[0].uid, Some(7));
    assert_eq!(fetched[0].flags.as_deref(), Some(&[Flag::Seen][..]));
    assert_eq!(fetched[0].body_sections.len(), 1);
    assert_eq!(
        fetched[0].body_sections[0].data.as_deref(),
        Some(&b"HELLO WORLD"[..])
    );
}

#[tokio::test]
async fn unsolicited_responses_during_noop_become_typed_events() {
    let (conn, mut server) =
        crate::connection::test_support::driver_pair(&preauth_greeting("IMAP4rev1")).await;

    let script = tokio::spawn(async move {
        let line = read_line(&mut server).await;
        assert!(line.contains("NOOP"), "unexpected command: {line}");
        let tag = tag_of(&line).to_owned();
        respond(
            &mut server,
            &format!("* 12 EXISTS\r\n* 4 EXPUNGE\r\n{tag} OK NOOP completed\r\n"),
        )
        .await;
        server
    });

    conn.noop(Duration::from_secs(5)).await.unwrap();
    let _server = script.await.unwrap();

    let events = conn.drain_events().await;
    assert!(
        events
            .iter()
            .any(|e| matches!(e, typed_event::TypedEvent::Exists(12))),
        "EXISTS was not surfaced: {events:?}"
    );
    assert!(
        events
            .iter()
            .any(|e| matches!(e, typed_event::TypedEvent::Expunge(4))),
        "EXPUNGE was not surfaced: {events:?}"
    );
}

#[tokio::test]
async fn capability_response_updates_the_cached_snapshot() {
    let (conn, mut server) =
        crate::connection::test_support::driver_pair(&preauth_greeting("IMAP4rev1")).await;
    assert!(!conn.capabilities().contains(&Capability::Idle));

    let script = tokio::spawn(async move {
        let line = read_line(&mut server).await;
        assert!(line.contains("CAPABILITY"), "unexpected command: {line}");
        let tag = tag_of(&line).to_owned();
        respond(
            &mut server,
            &format!("* CAPABILITY IMAP4rev1 IDLE MOVE\r\n{tag} OK CAPABILITY completed\r\n"),
        )
        .await;
        server
    });

    let caps = conn.capability(Duration::from_secs(5)).await.unwrap();
    let _server = script.await.unwrap();

    assert!(caps.contains(&Capability::Idle));
    assert!(conn.capabilities().contains(&Capability::Move));
}

#[tokio::test]
async fn bye_mid_command_preserves_the_response_code_without_waiting_for_close() {
    let (conn, mut server) =
        crate::connection::test_support::driver_pair(&preauth_greeting("IMAP4rev1")).await;
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();

    let script = tokio::spawn(async move {
        let _line = read_line(&mut server).await;
        respond(&mut server, "* BYE [UNAVAILABLE] server shutting down\r\n").await;
        let _ = release_rx.await;
    });

    let err = conn
        .noop(Duration::from_secs(5))
        .await
        .expect_err("the command cannot complete after BYE");

    assert!(
        matches!(
            err,
            Error::Bye {
                code: Some(ResponseCode::Unavailable),
                ..
            }
        ),
        "BYE must preserve its structured response code: {err:?}"
    );
    assert!(
        !conn.is_alive(),
        "BYE must close the driver command channel even while the peer stays open"
    );
    let _ = release_tx.send(());
    script.await.unwrap();
}

#[tokio::test]
async fn bye_carrying_a_capability_code_still_ends_the_command() {
    // `* BYE [CAPABILITY ...]` is a real shutdown shape (servers repeat their
    // pre-login capabilities on the way out). The response code must not
    // route the response away from the fatal-BYE lane.
    let (conn, mut server) =
        crate::connection::test_support::driver_pair(&preauth_greeting("IMAP4rev1")).await;
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();

    let script = tokio::spawn(async move {
        let _line = read_line(&mut server).await;
        respond(
            &mut server,
            "* BYE [CAPABILITY IMAP4rev1 MOVE] shutting down\r\n",
        )
        .await;
        let _ = release_rx.await;
    });

    let err = tokio::time::timeout(Duration::from_secs(5), conn.noop(Duration::from_secs(30)))
        .await
        .expect("BYE must end the command without waiting for the peer to close")
        .expect_err("the command cannot complete after BYE");

    assert!(
        matches!(
            err,
            Error::Bye {
                code: Some(ResponseCode::Capability(_)),
                ..
            }
        ),
        "BYE must preserve its structured response code: {err:?}"
    );
    assert!(
        !conn.is_alive(),
        "BYE must close the driver command channel"
    );
    let _ = release_tx.send(());
    script.await.unwrap();
}

#[tokio::test]
async fn starttls_rejects_an_empty_capability_snapshot_before_touching_the_driver() {
    // This models the plaintext `TlsMode::None` connection state. A missing
    // capability is not permission to attempt a downgrade-adjacent upgrade.
    let conn =
        crate::connection::test_support::detached(SessionState::NotAuthenticated, Vec::new(), &[]);
    let connector = native_tls::TlsConnector::builder().build().unwrap();

    assert!(matches!(
        conn.starttls_with_connector(connector, Duration::from_secs(1))
            .await,
        Err(Error::StartTlsUnavailable)
    ));
}

#[tokio::test]
async fn append_waits_for_the_continuation_before_sending_the_literal() {
    // No LITERAL+/LITERAL- advertised, so the APPEND literal is
    // synchronizing (RFC 3501 Section 4.3): the driver must stop after
    // `{n}\r\n` and wait for `+`.
    let (conn, mut server) =
        crate::connection::test_support::driver_pair(&preauth_greeting("IMAP4rev1 UIDPLUS")).await;

    let script = tokio::spawn(async move {
        let header = read_line(&mut server).await;
        assert!(header.contains("APPEND"), "unexpected command: {header}");
        assert!(
            header.trim_end().ends_with("{5}"),
            "expected a synchronizing literal marker, got: {header}"
        );
        assert!(
            header.starts_with('P'),
            "pre-built commands use the P tag prefix, got: {header}"
        );
        let tag = tag_of(&header).to_owned();

        // Nothing more may arrive until we grant the continuation.
        respond(&mut server, "+ Ready for literal data\r\n").await;

        let body = crate::connection::test_support::read_exact(&mut server, 5).await;
        assert_eq!(&body[..], b"HELLO");
        let trailer = read_line(&mut server).await;
        assert_eq!(trailer, "\r\n");

        respond(
            &mut server,
            &format!("{tag} OK [APPENDUID 4242 12] APPEND completed\r\n"),
        )
        .await;
        server
    });

    let uid = conn
        .append("INBOX", &[], None, b"HELLO", Duration::from_secs(5))
        .await
        .unwrap();
    let _server = script.await.unwrap();

    assert_eq!(uid, Some((4242, 12)));
}

#[tokio::test]
async fn append_uses_a_non_synchronizing_literal_under_literal_plus() {
    let (conn, mut server) = crate::connection::test_support::driver_pair(&preauth_greeting(
        "IMAP4rev1 LITERAL+ UIDPLUS",
    ))
    .await;

    let script = tokio::spawn(async move {
        let header = read_line(&mut server).await;
        assert!(
            header.trim_end().ends_with("{5+}"),
            "expected a LITERAL+ marker, got: {header}"
        );
        let tag = tag_of(&header).to_owned();
        // The payload follows without a continuation.
        let body = crate::connection::test_support::read_exact(&mut server, 5).await;
        assert_eq!(&body[..], b"HELLO");
        let trailer = read_line(&mut server).await;
        assert_eq!(trailer, "\r\n");
        respond(&mut server, &format!("{tag} OK APPEND completed\r\n")).await;
        server
    });

    let uid = conn
        .append("INBOX", &[], None, b"HELLO", Duration::from_secs(5))
        .await
        .unwrap();
    let _server = script.await.unwrap();
    assert_eq!(uid, None, "no APPENDUID means no id");
}

#[tokio::test]
async fn literal_plus_append_does_not_wait_on_a_marker_shaped_body_line() {
    let (conn, mut server) = crate::connection::test_support::driver_pair(&preauth_greeting(
        "IMAP4rev1 LITERAL+ UIDPLUS",
    ))
    .await;

    let script = tokio::spawn(async move {
        let header = read_line(&mut server).await;
        assert!(header.trim_end().ends_with("{11+}"));
        let tag = tag_of(&header).to_owned();
        let body = crate::connection::test_support::read_exact(&mut server, 11).await;
        assert_eq!(&body[..], b"abc{3}\r\ndef");
        assert_eq!(read_line(&mut server).await, "\r\n");
        respond(&mut server, &format!("{tag} OK APPEND completed\r\n")).await;
        server
    });

    conn.append("INBOX", &[], None, b"abc{3}\r\ndef", Duration::from_secs(5))
        .await
        .unwrap();
    let _server = script.await.unwrap();
}

#[tokio::test]
async fn append_rejects_oversized_messages_before_the_wire() {
    // RFC 7889: APPENDLIMIT is enforced client-side.
    let (conn, _server) =
        crate::connection::test_support::driver_pair(&preauth_greeting("IMAP4rev1 APPENDLIMIT=4"))
            .await;
    match conn
        .append("INBOX", &[], None, b"HELLO", Duration::from_secs(5))
        .await
    {
        Err(Error::AppendLimit { size, limit }) => {
            assert_eq!(size, 5);
            assert_eq!(limit, 4);
        }
        other => panic!("expected AppendLimit, got {other:?}"),
    }
}
