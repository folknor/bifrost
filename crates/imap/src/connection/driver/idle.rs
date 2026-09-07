use tokio::sync::oneshot;
use tracing::{debug, trace};

use crate::error::Error;
use crate::types::Command;
use crate::types::response::StatusKind;

use super::IdleTermination;
use super::event_sink;
use super::wire_send::{send_command_on_wire, wait_for_continuation};

/// Enter IDLE mode, read and publish events, exit on DONE or server
/// termination (RFC 2177 Sections 2-4).
///
/// 1. Sends the IDLE command on the wire (tagged, via the encoder).
/// 2. Waits for the `+` continuation (RFC 2177 Section 3).
/// 3. Enters a reading loop: `select!` between wire reads and `done_rx`.
///    - Untagged responses are emitted as events via `event_sink`.
///    - The tagged OK means the server terminated IDLE.
///    - `done_rx` fires when the handle wants to exit IDLE.
/// 4. On `done_rx`: sends `DONE\r\n` (untagged, RFC 2177 Section 3),
///    reads remaining responses until the tagged OK, returns `ClientDone`.
/// 5. On server-sent tagged OK: returns `ServerTerminated`.
pub(super) async fn run_idle(
    wire_reader: &mut super::super::wire::WireReader,
    state: &mut super::super::state::ProtocolState,
    tag_gen: &mut super::super::tag::TagGenerator,
    event_sink: &mut event_sink::DriverEventSink,
    done_rx: oneshot::Receiver<()>,
) -> Result<IdleTermination, Error> {
    // 1. Send IDLE command (RFC 2177 Section 2).
    let tag = send_command_on_wire(wire_reader, state, tag_gen, event_sink, &Command::Idle).await?;

    // 2. Wait for `+` continuation (RFC 2177 Section 3).
    wait_for_continuation(wire_reader, state, event_sink).await?;

    trace!(tag, "driver: entered IDLE mode");

    // 3. IDLE reading loop.
    let mut done_rx = done_rx;
    loop {
        // Opportunistic drain of pending critical events (D7).
        let _ = event_sink.drain_pending_nonblocking();

        // Use a flag to signal which branch won, avoiding borrow
        // conflicts between the done_rx handler and wire_reader.
        let done_signaled = tokio::select! {
            biased;
            _ = &mut done_rx => true,
            result = wire_reader.read_one(super::utf8_mode(state)) => {
                let resp = result?;
                let digest = state.apply_side_effects(&resp);
                match resp {
                    crate::types::Response::Tagged(t) if t.tag == tag => {
                        // RFC 2177 Section 3: server terminated IDLE.
                        super::emit_tagged_response_code_events(&t, event_sink);
                        // Check status. NO/BAD is an error, not a
                        // successful server termination.
                        match t.status {
                            StatusKind::Ok => {
                                trace!(tag, "driver: server terminated IDLE");
                                return Ok(IdleTermination::ServerTerminated);
                            }
                            StatusKind::No => {
                                return Err(Error::no_with_code(t.text, t.code));
                            }
                            StatusKind::Bad => {
                                return Err(Error::bad_with_code(t.text, t.code));
                            }
                        }
                    }
                    crate::types::Response::Tagged(t) => {
                        // Foreign tagged response during IDLE. This is a
                        // protocol anomaly; emit tagged response code events
                        // and otherwise ignore to preserve the connection.
                        super::emit_tagged_response_code_events(&t, event_sink);
                    }
                    crate::types::Response::Untagged(u) => {
                        super::process_untagged_as_event(digest, u, event_sink)?;
                    }
                    crate::types::Response::Continuation(_) => {
                        // A second continuation during IDLE is invalid;
                        // ignore it and keep waiting for events/DONE.
                        debug!(tag, "ignoring unexpected continuation during IDLE");
                    }
                    crate::types::Response::Greeting(_) => {
                        return Err(Error::Protocol("unexpected greeting during IDLE".into()));
                    }
                }
                false
            }
        };

        if done_signaled {
            // 4. Client requested exit. Send DONE (untagged, RFC 2177 Section3).
            trace!(tag, "driver: sending DONE");
            wire_reader.write_all(b"DONE\r\n").await?;
            drain_idle_responses(wire_reader, state, event_sink, &tag).await?;
            trace!(tag, "driver: exited IDLE mode");
            return Ok(IdleTermination::ClientDone);
        }
    }
}

/// After sending DONE, read until the tagged OK for the IDLE command
/// (RFC 2177 Section3).
async fn drain_idle_responses(
    wire_reader: &mut super::super::wire::WireReader,
    state: &mut super::super::state::ProtocolState,
    event_sink: &mut event_sink::DriverEventSink,
    tag: &str,
) -> Result<(), Error> {
    loop {
        let resp = wire_reader.read_one(super::utf8_mode(state)).await?;
        let digest = state.apply_side_effects(&resp);
        match resp {
            crate::types::Response::Tagged(t) if t.tag == tag => {
                super::emit_tagged_response_code_events(&t, event_sink);
                return match t.status {
                    StatusKind::Ok => Ok(()),
                    StatusKind::No => Err(Error::no_with_code(t.text, t.code)),
                    StatusKind::Bad => Err(Error::bad_with_code(t.text, t.code)),
                };
            }
            crate::types::Response::Tagged(t) => {
                // Foreign tagged response while draining IDLE. Emit
                // critical response code events and ignore otherwise.
                super::emit_tagged_response_code_events(&t, event_sink);
            }
            crate::types::Response::Untagged(u) => {
                super::process_untagged_as_event(digest, u, event_sink)?;
            }
            crate::types::Response::Continuation(_) => {
                // Ignore unexpected continuation while draining DONE.
            }
            crate::types::Response::Greeting(_) => {
                return Err(Error::Protocol(
                    "unexpected greeting during IDLE drain".into(),
                ));
            }
        }
    }
}
