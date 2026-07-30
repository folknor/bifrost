use std::sync::Arc;

use tracing::debug;

use crate::error::Error;
use crate::types::Command;
use crate::types::response::Capability;

use super::super::dispatch::{CapabilityConsumer, TaggedOkConsumer};
use super::super::{CompressedStream, ImapStream, InnerStream, SessionState};
use super::{ConsumerErased, DriverConsumer, UpgradePayload, event_sink};

/// Execute a stream upgrade atomically.
///
/// Dispatches to the appropriate upgrade handler based on the payload.
/// The driver runs the protocol command internally (using a
/// `TaggedOkConsumer`), then atomically swaps the stream using the
/// `Poisoned` sentinel (I9, I10).
pub(super) async fn run_upgrade(
    wire_reader: &mut super::super::wire::WireReader,
    state: &mut super::super::state::ProtocolState,
    tag_gen: &mut super::super::tag::TagGenerator,
    event_sink: &mut event_sink::DriverEventSink,
    payload: UpgradePayload,
) -> Result<(), Error> {
    match payload {
        UpgradePayload::StartTls {
            tls_connector,
            server_name,
        } => {
            run_starttls_upgrade(
                wire_reader,
                state,
                tag_gen,
                event_sink,
                tls_connector,
                server_name,
            )
            .await
        }
        UpgradePayload::Compress => {
            run_compress_upgrade(wire_reader, state, tag_gen, event_sink).await
        }
    }
}

/// STARTTLS upgrade (RFC 3501 Section 6.2.1 / RFC 9051 Section 6.2.1).
///
/// 1. Send STARTTLS, await tagged OK.
/// 2. Verify the wire buffer is empty (B10 fix: no injected bytes).
/// 3. `mem::replace` the reader with a `Poisoned`-stream reader. No
///    `.await` between the buffer check and the replace.
/// 4. TLS handshake (may suspend). If the handshake fails or the
///    future is cancelled, the reader stays wrapping `Poisoned`
///    forever and the connection is dead (I9).
/// 5. Install a fresh `WireReader` on the new TLS stream. The fresh
///    reader has an empty buffer; the old buffer was dropped with
///    the old reader in step 3 (I10).
/// 6. Re-fetch capabilities (RFC 3501 Section6.2.1).
pub(in crate::connection) async fn run_starttls_upgrade(
    wire_reader: &mut super::super::wire::WireReader,
    state: &mut super::super::state::ProtocolState,
    tag_gen: &mut super::super::tag::TagGenerator,
    event_sink: &mut event_sink::DriverEventSink,
    tls_connector: native_tls::TlsConnector,
    server_name: String,
) -> Result<(), Error> {
    // Step 1: Send STARTTLS command, await tagged OK (RFC 3501 Section6.2.1).
    let consumer =
        DriverConsumer::Regular(Box::new(TaggedOkConsumer::default()) as Box<dyn ConsumerErased>);
    super::run_one_command(
        wire_reader,
        state,
        tag_gen,
        event_sink,
        Command::StartTls,
        consumer,
    )
    .await?;

    // Step 2: Verify buffer is empty BEFORE the swap (B10 fix).
    // RFC 3501 Section6.2.1: After STARTTLS OK, the client MUST discard
    // cached data. Extra bytes here could be injected by a MITM
    // before TLS was established.
    // No .await between this check and the mem::replace below.
    if !wire_reader.buffer_is_empty() {
        let metering = wire_reader.metering();
        *wire_reader =
            super::super::wire::WireReader::with_metering(ImapStream::Poisoned, metering);
        state.apply_infrastructure_failure();
        return Err(Error::Protocol(
            "STARTTLS: unexpected bytes in buffer at upgrade boundary \
             (possible MITM; RFC 3501 Section 6.2.1)"
                .into(),
        ));
    }

    // Step 3: Atomic swap. Replace the reader with one on a Poisoned
    // stream. The old reader is consumed, its buffer is dropped, and
    // we get the old stream back (I10).
    let metering = wire_reader.metering();
    let old_reader = std::mem::replace(
        wire_reader,
        super::super::wire::WireReader::with_metering(ImapStream::Poisoned, metering.clone()),
    );
    let old_stream = old_reader.into_stream();
    let Some(tcp) = old_stream.into_tcp() else {
        // Should be unreachable: the handle validates the stream
        // type before submitting the upgrade. Defensive: leave the
        // connection dead (Poisoned is already installed).
        state.apply_infrastructure_failure();
        return Err(Error::Protocol(
            "STARTTLS requires a plain TCP stream (already TLS or compressed)".into(),
        ));
    };

    // Step 4: TLS handshake (may suspend). If the handshake fails
    // or the future is cancelled, wire_reader stays wrapping Poisoned
    // forever and the connection is dead (I9).
    let connector = tokio_native_tls::TlsConnector::from(tls_connector);
    let tls_stream = match connector.connect(&server_name, tcp).await {
        Ok(s) => s,
        Err(e) => {
            // TLS handshake failed; connection is dead (Poisoned stays).
            state.apply_infrastructure_failure();
            return Err(Error::Io {
                source: Arc::new(std::io::Error::other(e)),
                attempt: None,
            });
        }
    };

    // Step 5: Install a fresh WireReader on the new TLS stream.
    // The reader has a fresh empty buffer; the old buffer was
    // dropped with old_reader in Step 3 (I10).
    *wire_reader =
        super::super::wire::WireReader::with_metering(ImapStream::Tls(tls_stream), metering);

    // Step 6: Re-read capabilities after TLS upgrade (RFC 3501 Section6.2.1).
    state.apply_capability_fetch(Vec::new());
    let cap_consumer =
        DriverConsumer::Regular(Box::new(CapabilityConsumer::default()) as Box<dyn ConsumerErased>);
    let result = super::run_one_command(
        wire_reader,
        state,
        tag_gen,
        event_sink,
        Command::Capability,
        cap_consumer,
    )
    .await?;
    let caps = result
        .downcast::<Vec<Capability>>()
        .map_err(|_| Error::Internal("CapabilityConsumer output downcast failed".into()))?;
    state.apply_capability_fetch(*caps);

    debug!("STARTTLS upgrade complete (RFC 3501 Section 6.2.1)");
    Ok(())
}

/// COMPRESS=DEFLATE upgrade (RFC 4978).
///
/// 1. Send COMPRESS, await tagged OK.
/// 2. Take remaining buffer bytes (already compressed data).
/// 3. `mem::replace` the reader with a `Poisoned`-stream reader.
/// 4. Wrap the old stream in a `CompressedStream`.
/// 5. Install a fresh `WireReader` on the compressed stream,
///    preserving any buffered compressed bytes.
async fn run_compress_upgrade(
    wire_reader: &mut super::super::wire::WireReader,
    state: &mut super::super::state::ProtocolState,
    tag_gen: &mut super::super::tag::TagGenerator,
    event_sink: &mut event_sink::DriverEventSink,
) -> Result<(), Error> {
    // Step 1: Send COMPRESS command, await tagged OK (RFC 4978 Section4).
    let consumer =
        DriverConsumer::Regular(Box::new(TaggedOkConsumer::default()) as Box<dyn ConsumerErased>);
    super::run_one_command(
        wire_reader,
        state,
        tag_gen,
        event_sink,
        Command::Compress,
        consumer,
    )
    .await?;

    // Step 2: Take remaining buffer bytes. They are compressed data
    // that must be preserved in the new CompressedStream's raw read
    // buffer (RFC 4978 Section3: server begins compressing immediately
    // after the CRLF ending the tagged OK).
    let remaining = wire_reader.take_buffer();

    // Step 3: Atomic swap with Poisoned sentinel.
    let metering = wire_reader.metering();
    let old_reader = std::mem::replace(
        wire_reader,
        super::super::wire::WireReader::with_metering(ImapStream::Poisoned, metering.clone()),
    );
    let old_stream = old_reader.into_stream();

    // Step 4: Wrap the old stream in a CompressedStream.
    // After the mem::replace above, Poisoned is installed. If any of
    // these error paths fire, the connection is dead. Transition state
    // to Logout so `require_state` rejects subsequent commands cleanly.
    let inner = match old_stream {
        ImapStream::Plain(tcp) => InnerStream::Plain(tcp),
        ImapStream::Tls(tls) => InnerStream::Tls(tls),
        ImapStream::Compressed(_) => {
            state.apply_infrastructure_failure();
            return Err(Error::Protocol(
                "COMPRESS=DEFLATE already active on this connection".into(),
            ));
        }
        ImapStream::Poisoned => {
            state.apply_infrastructure_failure();
            return Err(Error::Protocol(
                "stream poisoned; connection is dead".into(),
            ));
        }
        #[cfg(test)]
        ImapStream::Memory(_) => {
            state.apply_infrastructure_failure();
            return Err(Error::Protocol(
                "COMPRESS=DEFLATE not supported on in-memory test streams".into(),
            ));
        }
    };

    // Step 5: Build the CompressedStream and install.
    // RFC 4978 Section3: the server begins compressing immediately after the
    // CRLF ending the tagged OK.
    let mut compressed = CompressedStream::new(inner);
    if !remaining.is_empty() {
        compressed.raw_read_buf.extend_from_slice(&remaining);
    }
    *wire_reader =
        super::super::wire::WireReader::with_metering(ImapStream::Compressed(compressed), metering);

    debug!("COMPRESS=DEFLATE activated (RFC 4978)");
    Ok(())
}

/// Best-effort LOGOUT on graceful shutdown (RFC 3501 Section6.1.3).
///
/// Sends LOGOUT and reads the BYE/OK response. Errors are ignored; the
/// connection is being torn down and the caller has already dropped `cmd_tx`.
pub(super) async fn logout_best_effort(
    wire_reader: &mut super::super::wire::WireReader,
    state: &mut super::super::state::ProtocolState,
    tag_gen: &mut super::super::tag::TagGenerator,
    event_sink: &mut event_sink::DriverEventSink,
) -> Result<(), Error> {
    if state.session_state() == SessionState::Logout {
        return Ok(());
    }
    let tag = tag_gen.next();
    // LOGOUT is a trivial command with no literals; write raw bytes.
    let logout_line = format!("{tag} LOGOUT\r\n");
    wire_reader.write_all(logout_line.as_bytes()).await?;

    // Read responses until the tagged OK or an error. Apply side
    // effects so state transitions to Logout on the BYE/tagged OK.
    loop {
        let utf8 = super::utf8_mode(state);
        let resp = wire_reader.read_one(utf8).await?;
        let digest = state.apply_side_effects(&resp);
        match resp {
            crate::types::Response::Tagged(t) if t.tag == tag => break,
            crate::types::Response::Tagged(_) => break,
            crate::types::Response::Untagged(u) => {
                let code_emitted = super::emit_untagged_response_code_events(&u, event_sink);
                super::short_circuit_on_bye(digest, &u)?;
                if !code_emitted {
                    let _ = event_sink.emit((*u).into());
                }
            }
            crate::types::Response::Continuation(_) | crate::types::Response::Greeting(_) => {}
        }
    }
    Ok(())
}
