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
        matches!(inner, Err(Error::DriverPanicked { .. } | Error::DriverGone { .. })),
        "post-panic command should be DriverPanicked or DriverGone, got: {inner:?}"
    );
}
