use bifrost_types::TransmissionState;
use bytes::{Bytes, BytesMut};
use tracing::trace;

use crate::codec::encode::{LiteralMode, encode_command};
use crate::error::Error;
use crate::types::Command;
use crate::types::response::{Capability, StatusKind};

use super::event_sink;
use super::pipeline::{PipelineRouting, Routed, route_pipeline_response};

/// Whether a command's bytes all reached the wire, or the server refused a
/// synchronizing literal first.
///
/// Only reachable as `Rejected` in pipeline context: a refusal there is the
/// rejected command's ordinary per-command result, already routed into its
/// result slot, and the batch goes on to the next command. Outside the
/// pipeline the refusal is still returned as an error, so single-command
/// dispatch and IDLE only ever see `Sent`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum SendOutcome {
    Sent,
    Rejected,
}

/// Whether the server granted the literal or refused the command.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ContinuationOutcome {
    Granted,
    Rejected,
}

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
            send_with_literal_sync(wire_reader, state, event_sink, &patched, None).await?;
        }
        LiteralMode::LiteralMinus => {
            // LITERAL- path (RFC 7888 Section5): patch small (<=4096 byte)
            // literals to non-synchronizing; larger ones stay sync.
            let flat = encoded.into_buf();
            let patched =
                super::super::patch_small_literals_to_plus_with_binary(&flat, allow_literal8);
            send_with_literal_sync(wire_reader, state, event_sink, &patched, None).await?;
        }
        LiteralMode::Synchronizing => {
            // No literal extension: all literals are synchronizing
            // (RFC 3501 Section4.3). Use pre-split segments from the encoder.
            send_encoded_segments(wire_reader, state, event_sink, encoded.segments(), None).await?;
        }
    }
    Ok(tag)
}

/// Send bytes that may contain synchronizing literals, handling
/// continuation requests at each literal boundary (RFC 3501 Section4.3).
///
/// The driver owns this send path outright. It was lifted out of the
/// pre-driver connection layer, which retains no copy of it.
pub(super) async fn send_with_literal_sync(
    wire_reader: &mut super::super::wire::WireReader,
    state: &mut super::super::state::ProtocolState,
    event_sink: &mut event_sink::DriverEventSink,
    buf: &[u8],
    mut routing: Option<&mut PipelineRouting<'_>>,
) -> Result<SendOutcome, Error> {
    let mut pos = 0;
    while pos < buf.len() {
        if let Some((marker_end, literal_size)) = super::super::find_literal_boundary(&buf[pos..]) {
            // marker_end is the offset past `\r\n` within buf[pos..]
            let Some(send_end) = pos.checked_add(marker_end) else {
                return Err(Error::Internal(
                    "synchronizing literal marker offset overflowed command buffer".into(),
                ));
            };
            let Some(body_end) = send_end
                .checked_add(literal_size)
                .filter(|&end| end <= buf.len())
            else {
                return Err(Error::Internal(
                    "synchronizing literal marker exceeds command buffer".into(),
                ));
            };
            wire_reader
                .write_all(&buf[pos..send_end])
                .await
                .map_err(|e| e.with_attempt(TransmissionState::Unsent))?;
            if wait_for_continuation(wire_reader, state, event_sink, routing.as_deref_mut()).await?
                == ContinuationOutcome::Rejected
            {
                // The server refused this literal. Its remaining bytes - the
                // body and every trailing byte of command syntax after it -
                // must not be written: the server has ended this command's
                // parsing state and would read them as a new command line
                // (RFC 3501 Section4.3).
                return Ok(SendOutcome::Rejected);
            }
            // Send the literal body data (RFC 3501 Section4.3).
            // After the continuation is received the server is expecting
            // our literal bytes, so a send failure here is InFlight: the
            // preceding pre-literal bytes were accepted.
            wire_reader
                .write_all(&buf[send_end..body_end])
                .await
                .map_err(|e| e.with_attempt(TransmissionState::InFlight))?;
            pos = body_end;
        } else {
            // No more literals; send the rest.
            wire_reader
                .write_all(&buf[pos..])
                .await
                .map_err(|e| e.with_attempt(TransmissionState::Unsent))?;
            break;
        }
    }
    Ok(SendOutcome::Sent)
}

/// Send pre-split [`EncodedCommand`](crate::codec::encode::EncodedCommand)
/// segments, waiting for a `+` continuation response between each pair
/// of consecutive segments (RFC 3501 Section4.3).
///
/// The driver owns this send path outright. It was lifted out of the
/// pre-driver connection layer, which retains no copy of it.
pub(super) async fn send_encoded_segments(
    wire_reader: &mut super::super::wire::WireReader,
    state: &mut super::super::state::ProtocolState,
    event_sink: &mut event_sink::DriverEventSink,
    segments: &[BytesMut],
    mut routing: Option<&mut PipelineRouting<'_>>,
) -> Result<SendOutcome, Error> {
    for (i, segment) in segments.iter().enumerate() {
        // Whether this segment is Unsent or InFlight depends on whether a
        // prior continuation was received. The first segment is always
        // Unsent. Subsequent segments come after a server `+`, so their
        // send failures are InFlight (the server already ACKed the prefix).
        let state_for_segment = if i == 0 {
            TransmissionState::Unsent
        } else {
            TransmissionState::InFlight
        };
        wire_reader
            .write_all(segment)
            .await
            .map_err(|e| e.with_attempt(state_for_segment))?;
        // After every segment except the last, wait for `+`
        // (RFC 3501 Section4.3).
        if i + 1 < segments.len()
            && wait_for_continuation(wire_reader, state, event_sink, routing.as_deref_mut()).await?
                == ContinuationOutcome::Rejected
        {
            // Refused: abandon every remaining segment of this command. See
            // `send_with_literal_sync` for why the remainder must not go out.
            return Ok(SendOutcome::Rejected);
        }
    }
    Ok(SendOutcome::Sent)
}

/// Send the segments of a [`ChunkedCommand`](crate::codec::encode::ChunkedCommand),
/// waiting for a `+` continuation between each pair of consecutive segments
/// (RFC 3501 Section 4.3).
///
/// This is the send half of the ownership-preserving encoding: a message body
/// is written straight from the caller's `Bytes`, never gathered into a
/// command buffer. Each chunk is its own write, which is a flush per chunk, so
/// the encoder keeps chunks few (command syntax is coalesced; a body is one
/// chunk).
///
/// Transmission evidence follows [`send_encoded_segments`]: a failure in the
/// first segment is `Unsent`, and any later segment comes after a server `+`
/// and is `InFlight`. That is deliberately stricter for a non-synchronizing
/// command than the flat-buffer path, which stamps its trailing write `Unsent`:
/// APPEND is non-idempotent, and once a body has been offered to the socket,
/// claiming "the server never saw it" is the unsafe direction.
///
/// Only reachable without pipeline routing, so a refused continuation is an
/// ordinary error from [`wait_for_continuation`] and the `Rejected` outcome
/// cannot occur; it is still handled rather than assumed.
pub(super) async fn send_chunked_segments(
    wire_reader: &mut super::super::wire::WireReader,
    state: &mut super::super::state::ProtocolState,
    event_sink: &mut event_sink::DriverEventSink,
    segments: &[Vec<Bytes>],
) -> Result<(), Error> {
    for (i, segment) in segments.iter().enumerate() {
        let state_for_segment = if i == 0 {
            TransmissionState::Unsent
        } else {
            TransmissionState::InFlight
        };
        for chunk in segment {
            // An empty message body is a legal zero-length literal; there is
            // nothing to write for it.
            if chunk.is_empty() {
                continue;
            }
            wire_reader
                .write_all(chunk)
                .await
                .map_err(|e| e.with_attempt(state_for_segment))?;
        }
        if i + 1 < segments.len()
            && wait_for_continuation(wire_reader, state, event_sink, None).await?
                == ContinuationOutcome::Rejected
        {
            return Err(Error::Internal(
                "literal continuation refused outside a pipeline".into(),
            ));
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
/// The driver owns this wait outright. It was lifted out of the pre-driver
/// connection layer, which retains no copy of it.
///
/// This wait has two modes, and which one applies is decided entirely by
/// whether the caller has other commands outstanding.
///
/// With `routing: None` - single-command dispatch and IDLE - exactly one
/// command is on the wire, so ANY tagged response is that command's, and any
/// untagged response has no consumer to route to and becomes an event.
///
/// With `routing: Some(..)` the caller is `run_pipeline_batch`, which sends
/// command by command whenever a literal is synchronizing (`literal_mode` is
/// `Synchronizing`, or `LiteralMinus` with a literal too large for
/// `patch_small_literals_to_plus_with_binary` to patch). Commands already on
/// the wire then have tags still pending, and their responses arrive HERE. So
/// this wait routes them through the pipeline's own router rather than
/// answering them itself:
///
/// * a tagged response for an earlier command is that command's completion,
///   routed into its result slot;
/// * an untagged response solicited by an earlier command reaches the
///   consumer that asked for it;
/// * the tagged `NO`/`BAD` of the command being written is its own ordinary
///   per-command result, and the wait answers `Rejected` so the sender
///   abandons the rest of that command rather than failing the batch;
/// * a tagged `OK` for the command being written stays a protocol error: it
///   claims successful execution of a command the server has not received.
///
/// Both halves matter. Answering a foreign tagged response here destroyed an
/// earlier command's real result and reported its `NO` as a failure of the
/// command being written; answering a foreign untagged response as an
/// anonymous event was worse, because the batch then COMPLETED and the caller
/// got a silently short result with no error at all.
pub(super) async fn wait_for_continuation(
    wire_reader: &mut super::super::wire::WireReader,
    state: &mut super::super::state::ProtocolState,
    event_sink: &mut event_sink::DriverEventSink,
    mut routing: Option<&mut PipelineRouting<'_>>,
) -> Result<ContinuationOutcome, Error> {
    loop {
        let utf8 = super::utf8_mode(state);
        // We have already sent the pre-literal bytes; a transport failure
        // reading the server's continuation grant is InFlight.
        let resp = wire_reader
            .read_one(utf8)
            .await
            .map_err(|e| e.with_attempt(TransmissionState::InFlight))?;

        if let Some(routing) = routing.as_deref_mut() {
            // The router applies side effects itself, exactly once.
            match route_pipeline_response(state, event_sink, routing, resp)? {
                Routed::Continuation => return Ok(ContinuationOutcome::Granted),
                Routed::OwnTagRejected => return Ok(ContinuationOutcome::Rejected),
                Routed::Continue => continue,
            }
        }

        let digest = state.apply_side_effects(&resp);
        match resp {
            crate::types::Response::Continuation(_) => return Ok(ContinuationOutcome::Granted),
            crate::types::Response::Tagged(t) => {
                // Server rejected the command before the literal was
                // sent. The server processed the prefix, so this is
                // Acknowledged. Side effects already applied (RFC 3501 Section4.3).
                super::emit_tagged_response_code_events(&t, event_sink);
                return match t.status {
                    // NO/BAD are tagged responses - inherently Acknowledged.
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
                // (I13): the shared arm emits alert/notification overflow
                // before BYE handling, so ALERT codes on BYE responses are
                // not lost.
                super::process_untagged_as_event(digest, u, event_sink)?;
            }
            crate::types::Response::Greeting(_) => {
                return Err(Error::Protocol("unexpected greeting".into()));
            }
        }
    }
}
