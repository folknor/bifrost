use bytes::BytesMut;
use tracing::{trace, warn};

use crate::codec::encode::{LiteralMode, encode_command};
use crate::error::Error;
use crate::types::Command;
use crate::types::response::{Capability, StatusKind, UntaggedResponse};

use super::event_sink;

/// Encode and send a command over the wire, handling literal
/// synchronization (RFC 3501 Section4.3).
///
/// Returns the generated tag on success. Mirrors
/// `ImapConnection::send_command` adapted for the driver's
/// decomposed primitives.
pub(super) async fn send_command_on_wire(
    wire_reader: &mut super::super::wire::WireReader,
    state: &mut super::super::state::ProtocolState,
    tag_gen: &mut super::super::tag::TagGenerator,
    event_sink: &mut event_sink::DriverEventSink,
    cmd: &Command,
) -> Result<String, Error> {
    let tag = tag_gen.next();
    trace!(tag, ?cmd, "driver: sending IMAP command");
    let opts = super::build_encode_options(state);
    let encoded = encode_command(&tag, cmd, &opts)?;

    let allow_literal8 =
        state.capabilities().contains(&Capability::Binary) && !super::is_rev2(state);

    match opts.literal_mode {
        LiteralMode::LiteralPlus => {
            // LITERAL+ path (RFC 7888 Section4): patch all synchronizing
            // markers to non-synchronizing and send in one shot.
            let flat = encoded.into_buf();
            let patched = super::super::patch_literals_to_plus_with_binary(&flat, allow_literal8);
            send_with_literal_sync(wire_reader, state, event_sink, &patched).await?;
        }
        LiteralMode::LiteralMinus => {
            // LITERAL- path (RFC 7888 Section5): patch small (<=4096 byte)
            // literals to non-synchronizing; larger ones stay sync.
            let flat = encoded.into_buf();
            let patched =
                super::super::patch_small_literals_to_plus_with_binary(&flat, allow_literal8);
            send_with_literal_sync(wire_reader, state, event_sink, &patched).await?;
        }
        LiteralMode::Synchronizing => {
            // No literal extension: all literals are synchronizing
            // (RFC 3501 Section4.3). Use pre-split segments from the encoder.
            send_encoded_segments(wire_reader, state, event_sink, encoded.segments()).await?;
        }
    }
    Ok(tag)
}

/// Send bytes that may contain synchronizing literals, handling
/// continuation requests at each literal boundary (RFC 3501 Section4.3).
///
/// Mirrors `ImapConnection::send_with_literal_sync`.
pub(super) async fn send_with_literal_sync(
    wire_reader: &mut super::super::wire::WireReader,
    state: &mut super::super::state::ProtocolState,
    event_sink: &mut event_sink::DriverEventSink,
    buf: &[u8],
) -> Result<(), Error> {
    let mut pos = 0;
    while pos < buf.len() {
        if let Some((marker_end, literal_size)) = super::super::find_literal_boundary(&buf[pos..]) {
            // marker_end is the offset past `\r\n` within buf[pos..]
            let send_end = pos + marker_end;
            wire_reader.write_all(&buf[pos..send_end]).await?;
            wait_for_continuation(wire_reader, state, event_sink).await?;
            // Send the literal body data (RFC 3501 Section4.3).
            wire_reader
                .write_all(&buf[send_end..send_end + literal_size])
                .await?;
            pos = send_end + literal_size;
        } else {
            // No more literals; send the rest.
            wire_reader.write_all(&buf[pos..]).await?;
            break;
        }
    }
    Ok(())
}

/// Send pre-split [`EncodedCommand`](crate::codec::encode::EncodedCommand)
/// segments, waiting for a `+` continuation response between each pair
/// of consecutive segments (RFC 3501 Section4.3).
///
/// Mirrors `ImapConnection::send_encoded_segments`.
pub(super) async fn send_encoded_segments(
    wire_reader: &mut super::super::wire::WireReader,
    state: &mut super::super::state::ProtocolState,
    event_sink: &mut event_sink::DriverEventSink,
    segments: &[BytesMut],
) -> Result<(), Error> {
    for (i, segment) in segments.iter().enumerate() {
        wire_reader.write_all(segment).await?;
        // After every segment except the last, wait for `+`
        // (RFC 3501 Section4.3).
        if i + 1 < segments.len() {
            wait_for_continuation(wire_reader, state, event_sink).await?;
        }
    }
    Ok(())
}

/// Wait for a server continuation response (`+ ...`) during
/// synchronizing-literal sends (RFC 3501 Section4.3, Section7.5).
///
/// Reads responses until the definitive signal arrives. Untagged
/// responses are emitted as events. BYE transitions state to Logout.
///
/// Mirrors `ImapConnection::wait_for_continuation`.
pub(super) async fn wait_for_continuation(
    wire_reader: &mut super::super::wire::WireReader,
    state: &mut super::super::state::ProtocolState,
    event_sink: &mut event_sink::DriverEventSink,
) -> Result<(), Error> {
    loop {
        let utf8 = super::utf8_mode(state);
        let resp = wire_reader.read_one(utf8).await?;
        let digest = state.apply_side_effects(&resp);
        match resp {
            crate::types::Response::Continuation(_) => return Ok(()),
            crate::types::Response::Tagged(t) => {
                // Server rejected the command before the literal was
                // sent. Side effects already applied (RFC 3501 Section4.3).
                super::emit_tagged_response_code_events(&t, event_sink);
                return match t.status {
                    StatusKind::No => Err(Error::no_with_code(t.text, t.code)),
                    StatusKind::Bad => Err(Error::bad_with_code(t.text, t.code)),
                    StatusKind::Ok => Err(Error::Protocol(
                        "unexpected OK before literal continuation \
                         (RFC 3501 Section4.3)"
                            .into(),
                    )),
                };
            }
            crate::types::Response::Untagged(u) => {
                // (I13): emit alert/notification overflow before
                // BYE handling to ensure ALERT codes on BYE responses
                // are not lost.
                let code_emitted = super::emit_untagged_response_code_events(&u, event_sink);

                // RFC 3501 Section7.1.5: BYE means the server is closing.
                // State already transitioned to Logout by
                // apply_side_effects.
                if digest.had_bye {
                    let (text, code) = match *u {
                        UntaggedResponse::Status { text, code, .. } => (text, code),
                        _ => (String::new(), None),
                    };
                    warn!(text, "received BYE during literal sync");
                    return Err(Error::bye_with_code(text, code));
                }
                if !code_emitted {
                    let _ = event_sink.emit((*u).into());
                }
            }
            crate::types::Response::Greeting(_) => {
                return Err(Error::Protocol("unexpected greeting".into()));
            }
        }
    }
}
