//! Driver task. Owns the `WireReader` and `ProtocolState` exclusively.
//! The public API submits commands over mpsc and awaits oneshot replies.
//!
//! The driver task is the only code that touches the wire reader and
//! protocol state directly. All command execution flows through
//! `run_one_command`, which runs the dispatch loop on decomposed
//! primitives (`WireReader`, `ProtocolState`, tag generator,
//! `DriverEventSink`).

use bytes::BytesMut;
use tokio::sync::{mpsc, oneshot, watch};
use tracing::trace;

use bifrost_types::TransmissionState;

use crate::codec::classification::{self, ClassificationContext, SolicitationRule};
use crate::codec::encode::{EncodeOptions, LiteralMode};
use crate::error::Error;
use crate::types::Command;
use crate::types::response::{Capability, UntaggedResponse};
use crate::types::validated::MailboxName;

use super::NotifyFlags;
use super::dispatch::{
    BackpressureState, Consumer, ConsumerContext, ContinuationConsumer, ContinuationReply,
    Finalized, StreamingConsumer,
};
mod events;
mod idle;
mod pipeline;
mod upgrade;
mod wire_send;

use events::{
    emit_tagged_response_code_events, emit_untagged_response_code_events,
    has_critical_response_code,
};
use idle::run_idle;
#[cfg(test)]
use pipeline::group_into_sub_batches;
use pipeline::run_pipeline;
use upgrade::{logout_best_effort, run_upgrade};
use wire_send::{send_command_on_wire, send_with_literal_sync};

pub(in crate::connection) use upgrade::run_starttls_upgrade;

/// Per-command result type for pipelined batch execution.
///
/// Each element corresponds to one command in the pipeline: `Ok` holds
/// the consumer's type-erased output, `Err` holds a per-command error
/// (e.g., server `NO` response).
pub(super) type PipelineResults = Vec<Result<Box<dyn std::any::Any + Send>, Error>>;

/// A command submitted to the driver task over `cmd_tx`.
///
/// Either a regular protocol command (with a type-erased consumer for
/// response routing) or a stream upgrade (STARTTLS / COMPRESS) that
/// the driver handles atomically without a consumer.
pub(super) enum DriverCommand {
    /// Regular command  -  the driver encodes/sends it and routes responses
    /// to the consumer via the classification-based dispatcher.
    Run {
        payload: DriverCommandPayload,
        consumer: DriverConsumer,
        result_tx: oneshot::Sender<Result<Box<dyn std::any::Any + Send>, Error>>,
    },
    /// Stream upgrade  -  the driver sends the protocol command, awaits
    /// the tagged OK, then atomically swaps the stream using the
    /// `Poisoned` sentinel (I9, I10). No consumer needed.
    Upgrade {
        payload: UpgradePayload,
        result_tx: oneshot::Sender<Result<Box<dyn std::any::Any + Send>, Error>>,
    },
    /// Pipelined batch  -  multiple commands submitted in one write,
    /// responses routed to each consumer by tag.
    Pipeline {
        /// Commands to encode and send as a batch.
        commands: Vec<Command>,
        /// Per-command consumers, parallel to `commands`.
        consumers: Vec<Box<dyn ConsumerErased>>,
        /// Channel for the per-command results.
        result_tx: oneshot::Sender<Result<PipelineResults, Error>>,
    },
    /// Set TCP keepalive socket options (OS-level, not IMAP protocol).
    ///
    /// Delegates to `WireReader::set_keepalive` which sets `setsockopt(2)`
    /// options on the underlying TCP socket. No data is sent on the wire.
    SetKeepalive {
        /// Keepalive configuration to apply.
        keepalive: super::TcpKeepalive,
        /// Channel for the result.
        result_tx: oneshot::Sender<Result<(), Error>>,
    },
    /// IDLE session  -  the driver enters IDLE mode on the wire, reads
    /// events and publishes them via the event sink, and exits when
    /// `done_rx` fires or the server terminates IDLE (RFC 2177).
    Idle {
        /// Handle signals DONE via this channel.
        done_rx: oneshot::Receiver<()>,
        /// Driver reports how IDLE ended.
        result_tx: oneshot::Sender<Result<IdleTermination, Error>>,
    },
}

/// How an IDLE session ended (RFC 2177 Section 3).
#[derive(Debug)]
pub(super) enum IdleTermination {
    /// Client sent DONE (normal exit).
    ClientDone,
    /// Server sent tagged OK (server-terminated IDLE).
    ServerTerminated,
}

/// Payload for a [`DriverCommand::Run`].
///
/// Standard commands are encoded by the driver via [`encode_command`].
/// Pre-built commands (APPEND/MULTIAPPEND) carry wire bytes built by
/// the handle side, which the driver sends with literal synchronization.
pub(super) enum DriverCommandPayload {
    /// Standard IMAP command  -  encoded and sent by the driver.
    Standard(Command),
    /// Pre-built wire bytes (APPEND / MULTIAPPEND).
    ///
    /// The handle builds the complete wire bytes (including the tag) and
    /// provides the tag separately so the driver can match the tagged
    /// response. The driver sends the bytes with literal synchronization
    /// and runs the response classification loop.
    PreBuilt {
        /// Complete wire bytes to send, including the tag prefix.
        wire_bytes: BytesMut,
        /// The tag embedded in `wire_bytes`, used for response matching.
        tag: String,
        /// Command kind for response classification.
        cmd_kind: crate::types::CommandKind,
        /// Optional mailbox target for classification context.
        cmd_target: Option<MailboxName>,
    },
}

/// Payload for a [`DriverCommand::Upgrade`].
///
/// Stream upgrades are handled atomically by the driver task using the
/// `Poisoned` sentinel pattern (I9, I10). The driver sends the protocol
/// command (STARTTLS / COMPRESS), awaits the tagged OK, then swaps the
/// stream without any `.await` between the buffer check and the swap.
pub(super) enum UpgradePayload {
    /// STARTTLS upgrade (RFC 3501 Section 6.2.1 / RFC 9051 Section 6.2.1).
    ///
    /// The driver sends STARTTLS, awaits OK, verifies the buffer is empty,
    /// mem-replaces the stream with `Poisoned`, performs the TLS handshake,
    /// and installs a fresh `WireReader` on the TLS stream. Capabilities
    /// are re-fetched after the upgrade.
    StartTls {
        /// TLS configuration for the handshake.
        tls_connector: native_tls::TlsConnector,
        /// Server name for TLS SNI and certificate verification.
        server_name: String,
    },
    /// COMPRESS=DEFLATE upgrade (RFC 4978).
    ///
    /// The driver sends COMPRESS, awaits OK, takes remaining buffer bytes
    /// (already compressed), wraps the stream in a `CompressedStream`,
    /// and installs a fresh `WireReader`.
    Compress,
}

/// Object-safe wrapper for [`Consumer`] that erases the `Output` type.
///
/// The blanket impl below bridges any `Consumer` whose `Output` is
/// `Send + 'static` into this trait by boxing the output.
pub(super) trait ConsumerErased: Send {
    /// Forwards to [`Consumer::on_response`].
    fn on_response(
        &mut self,
        resp: UntaggedResponse,
        notify_snapshot: NotifyFlags,
        ctx: &ConsumerContext,
    );

    /// Forwards to [`Consumer::finalize`], boxing the output.
    fn finalize_erased(
        self: Box<Self>,
        tagged: crate::types::response::TaggedResponse,
        ctx: &ConsumerContext,
    ) -> Result<Finalized<Box<dyn std::any::Any + Send>>, Error>;
}

/// Object-safe wrapper for streaming consumers that can await
/// downstream capacity before the driver reads more wire data.
pub(super) trait StreamingConsumerErased: ConsumerErased {
    fn backpressure_state_erased(&self) -> BackpressureState;

    fn reserve_capacity_erased(
        &mut self,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), Error>> + Send + '_>>;
}

impl<C: StreamingConsumer + 'static> StreamingConsumerErased for C
where
    C::Output: 'static,
{
    fn backpressure_state_erased(&self) -> BackpressureState {
        <C as StreamingConsumer>::backpressure_state(self)
    }

    fn reserve_capacity_erased(
        &mut self,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), Error>> + Send + '_>> {
        <C as StreamingConsumer>::reserve_capacity(self)
    }
}

/// Blanket impl that erases `C::Output` to `Box<dyn Any + Send>`.
impl<C: Consumer + 'static> ConsumerErased for C
where
    C::Output: 'static,
{
    fn on_response(
        &mut self,
        resp: UntaggedResponse,
        notify_snapshot: NotifyFlags,
        ctx: &ConsumerContext,
    ) {
        <C as Consumer>::on_response(self, resp, notify_snapshot, ctx);
    }

    fn finalize_erased(
        self: Box<Self>,
        tagged: crate::types::response::TaggedResponse,
        ctx: &ConsumerContext,
    ) -> Result<Finalized<Box<dyn std::any::Any + Send>>, Error> {
        let finalized = <C as Consumer>::finalize(self, tagged, ctx)?;
        Ok(Finalized {
            output: Box::new(finalized.output) as Box<dyn std::any::Any + Send>,
            reclassified_as_events: finalized.reclassified_as_events,
        })
    }
}

/// Object-safe wrapper for [`ContinuationConsumer`] that extends
/// [`ConsumerErased`] with continuation handling.
///
/// Used by AUTHENTICATE and future multi-round SASL commands that
/// expect `+` continuations during the response loop.
pub(super) trait ContinuationConsumerErased: ConsumerErased {
    /// Forwards to [`ContinuationConsumer::on_continuation`].
    fn on_continuation_erased(
        &mut self,
        cont: crate::types::response::ContinuationRequest,
        ctx: &ConsumerContext,
    ) -> Result<ContinuationReply, Error>;
}

/// Blanket impl that bridges any [`ContinuationConsumer`] into
/// [`ContinuationConsumerErased`].
impl<C: ContinuationConsumer + 'static> ContinuationConsumerErased for C
where
    C::Output: 'static,
{
    fn on_continuation_erased(
        &mut self,
        cont: crate::types::response::ContinuationRequest,
        ctx: &ConsumerContext,
    ) -> Result<ContinuationReply, Error> {
        <C as ContinuationConsumer>::on_continuation(self, cont, ctx)
    }
}

/// Type-erased consumer submitted to the driver task.
///
/// Either a regular consumer (errors on unexpected `+`) or a
/// continuation-aware consumer (routes `+` to `on_continuation`).
pub(super) enum DriverConsumer {
    /// Regular consumer  -  unexpected continuations are a protocol error.
    Regular(Box<dyn ConsumerErased>),
    /// Streaming consumer  -  regular command with async pre-read
    /// backpressure.
    StreamingRegular(Box<dyn StreamingConsumerErased>),
    /// Continuation consumer  -  `+` responses are routed to the
    /// consumer's `on_continuation` handler (AUTHENTICATE, SASL).
    WithContinuations(Box<dyn ContinuationConsumerErased>),
}

impl DriverConsumer {
    /// Forwards to [`ConsumerErased::on_response`].
    fn on_response(
        &mut self,
        resp: UntaggedResponse,
        notify_snapshot: NotifyFlags,
        ctx: &ConsumerContext,
    ) {
        match self {
            Self::Regular(c) => c.on_response(resp, notify_snapshot, ctx),
            Self::StreamingRegular(c) => c.on_response(resp, notify_snapshot, ctx),
            Self::WithContinuations(c) => c.on_response(resp, notify_snapshot, ctx),
        }
    }

    /// Forwards to [`ConsumerErased::finalize_erased`].
    fn finalize_erased(
        self,
        tagged: crate::types::response::TaggedResponse,
        ctx: &ConsumerContext,
    ) -> Result<Finalized<Box<dyn std::any::Any + Send>>, Error> {
        match self {
            Self::Regular(c) => c.finalize_erased(tagged, ctx),
            Self::StreamingRegular(c) => c.finalize_erased(tagged, ctx),
            Self::WithContinuations(c) => c.finalize_erased(tagged, ctx),
        }
    }

    async fn prepare_to_read(&mut self) -> Result<(), Error> {
        if let Self::StreamingRegular(c) = self
            && matches!(
                c.backpressure_state_erased(),
                BackpressureState::NeedsCapacity
            )
        {
            c.reserve_capacity_erased().await?;
        }
        Ok(())
    }

    /// Handle a `+` continuation. Returns `Err` for regular consumers
    /// (unexpected continuation is a protocol error per RFC 3501 Section7.5).
    fn on_continuation(
        &mut self,
        cont: crate::types::response::ContinuationRequest,
        ctx: &ConsumerContext,
    ) -> Result<ContinuationReply, Error> {
        match self {
            Self::Regular(_) => Err(Error::Protocol(
                "unexpected continuation during command that does not expect one".into(),
            )),
            Self::StreamingRegular(_) => Err(Error::Protocol(
                "unexpected continuation during streaming command".into(),
            )),
            Self::WithContinuations(c) => c.on_continuation_erased(cont, ctx),
        }
    }
}

/// Read-only snapshot of connection state published by the driver
/// via `watch::Sender`.
///
/// Holds session state, capabilities, notify flags, selected mailbox,
/// and enabled extensions (RFC 3501 Section3, Section7.2.1; RFC 5161 Section3.2;
/// RFC 5465 Section5.1-5.8).
#[derive(Debug, Clone)]
pub(super) struct ConnectionStateSnapshot {
    /// Current session state (RFC 3501 Section3).
    pub session_state: super::SessionState,
    /// Cached server capabilities (RFC 3501 Section7.2.1).
    pub capabilities: Vec<Capability>,
    /// Successfully `ENABLE`d extensions (RFC 5161 Section3.2).
    pub enabled: Vec<String>,
}

impl Default for ConnectionStateSnapshot {
    fn default() -> Self {
        Self {
            session_state: super::SessionState::NotAuthenticated,
            capabilities: Vec::new(),
            enabled: Vec::new(),
        }
    }
}

// ---------------------------------------------------------------------------
// Driver task body
// ---------------------------------------------------------------------------

/// Driver-task body. Owns the wire reader, protocol state, and tag generator.
///
/// Receives `DriverCommand`s from the mpsc, runs each command using
/// the classification-based dispatch loop, and publishes events via
/// [`DriverEventSink`](event_sink::DriverEventSink). Publishes state
/// snapshots via `watch::Sender` after every command.
///
/// The caller (typically `ImapConnection::connect`) creates the wire
/// reader and protocol state, reads the greeting, fetches initial
/// capabilities, and then passes the pre-initialized components here.
pub(super) async fn driver_task(
    mut wire_reader: super::wire::WireReader,
    mut state: super::state::ProtocolState,
    mut tag_gen: super::tag::TagGenerator,
    mut cmd_rx: mpsc::Receiver<DriverCommand>,
    state_tx: watch::Sender<ConnectionStateSnapshot>,
    mut event_sink: event_sink::DriverEventSink,
) {
    loop {
        // Opportunistic drain of pending critical events (D7).
        // Ignore CallerGone  -  the cmd_rx.recv() below will also
        // observe the caller being gone via channel close.
        let _ = event_sink.drain_pending_nonblocking();

        tokio::select! {
            biased;
            maybe_cmd = cmd_rx.recv() => {
                let Some(cmd) = maybe_cmd else { break; };
                match cmd {
                    DriverCommand::Run { payload, consumer, result_tx } => {
                        let result = match payload {
                            DriverCommandPayload::Standard(command) => {
                                run_one_command(
                                    &mut wire_reader,
                                    &mut state,
                                    &mut tag_gen,
                                    &mut event_sink,
                                    command,
                                    consumer,
                                ).await
                            }
                            DriverCommandPayload::PreBuilt {
                                wire_bytes, tag, cmd_kind, cmd_target,
                            } => {
                                run_prebuilt_command(
                                    &mut wire_reader,
                                    &mut state,
                                    &mut event_sink,
                                    wire_bytes,
                                    &tag,
                                    cmd_kind,
                                    cmd_target,
                                    consumer,
                                ).await
                            }
                        };
                        let _ = result_tx.send(result);
                    }
                    DriverCommand::Upgrade { payload, result_tx } => {
                        let result = run_upgrade(
                            &mut wire_reader,
                            &mut state,
                            &mut tag_gen,
                            &mut event_sink,
                            payload,
                        ).await;
                        let _ = result_tx.send(result.map(|()| {
                            Box::new(()) as Box<dyn std::any::Any + Send>
                        }));
                    }
                    DriverCommand::Pipeline { commands, consumers, result_tx } => {
                        let result = run_pipeline(
                            &mut wire_reader,
                            &mut state,
                            &mut tag_gen,
                            &mut event_sink,
                            commands,
                            consumers,
                        ).await;
                        let _ = result_tx.send(result);
                    }
                    DriverCommand::SetKeepalive { keepalive, result_tx } => {
                        let result = wire_reader.set_keepalive(&keepalive);
                        let _ = result_tx.send(result);
                        // No protocol state changed  -  skip snapshot publish.
                        continue;
                    }
                    DriverCommand::Idle { done_rx, result_tx } => {
                        let result = run_idle(
                            &mut wire_reader,
                            &mut state,
                            &mut tag_gen,
                            &mut event_sink,
                            done_rx,
                        ).await;
                        let _ = result_tx.send(result);
                    }
                }
                let _ = state_tx.send_replace(state.snapshot());

                // RFC 3501 Section3.4: once the session reaches Logout state
                // (either via BYE or tagged OK for LOGOUT), the
                // connection is closing. Exit the driver loop so
                // cmd_rx is dropped and subsequent submit calls
                // observe DriverGone instead of hanging.
                if state.session_state() == super::SessionState::Logout {
                    break;
                }
            }
        }
    }

    // Graceful shutdown: send LOGOUT if still authenticated.
    // Best-effort; ignore errors.
    let _ = logout_best_effort(&mut wire_reader, &mut state, &mut tag_gen).await;
}

// ---------------------------------------------------------------------------
// Command execution
// ---------------------------------------------------------------------------

/// Execute a single command through the classification-based dispatcher.
///
/// Encodes and sends the command (handling literal synchronization
/// per RFC 3501 Section4.3),
/// reads responses in a loop, classifies each untagged response via
/// [`classify`](crate::codec::classification::classify), and routes it
/// to either the consumer (solicited / ambiguous) or the event sink
/// (unsolicited / impossible). Errors on unexpected continuations
/// (RFC 3501 Section7.5).
pub(in crate::connection) async fn run_one_command(
    wire_reader: &mut super::wire::WireReader,
    state: &mut super::state::ProtocolState,
    tag_gen: &mut super::tag::TagGenerator,
    event_sink: &mut event_sink::DriverEventSink,
    cmd: Command,
    mut consumer: DriverConsumer,
) -> Result<Box<dyn std::any::Any + Send>, Error> {
    let cmd_kind = cmd.kind();
    let cmd_target: Option<MailboxName> = cmd.mailbox_target().cloned();

    // RFC 3501 Section6.1.3: tell apply_tagged to transition to Logout on
    // the tagged OK. Must be set before the dispatch loop so the
    // side-effect handler sees it when the tagged response arrives.
    if matches!(cmd, Command::Logout) {
        state.set_in_logout(true);
    }

    // RFC 3501 Section6.2.2, Section6.2.3: tell apply_tagged to transition to
    // Authenticated on the tagged OK for LOGIN or AUTHENTICATE.
    if matches!(cmd, Command::Login { .. } | Command::Authenticate { .. }) {
        state.set_in_auth(true);
    }

    // RFC 3501 Section6.3.1-Section6.3.2: tell apply_tagged to transition to
    // Selected on tagged OK, or Authenticated on tagged NO, for
    // SELECT or EXAMINE.
    if matches!(cmd, Command::Select { .. } | Command::Examine { .. }) {
        state.set_in_select(cmd_target.clone());
    }

    // RFC 3501 Section6.4.2, RFC 3691 Section3, RFC 9051 Section6.4.2: tell apply_tagged
    // to transition to Authenticated on tagged OK for CLOSE or UNSELECT.
    if matches!(cmd, Command::Close | Command::Unselect) {
        state.set_in_close(true);
    }

    // RFC 8437 Section2: tell apply_tagged to transition to NotAuthenticated
    // on tagged OK for UNAUTHENTICATE.
    if matches!(cmd, Command::Unauthenticate) {
        state.set_in_unauthenticate(true);
    }

    // RFC 5465 Section3: tell apply_tagged to update NOTIFY per-type flags
    // on tagged OK. NOTIFY SET computes flags from the registration
    // params; NOTIFY NONE resets to default (no notifications).
    if let Command::NotifySet(ref params) = cmd {
        let (list, status, metadata) = super::extensions::compute_notify_flags(params);
        state.set_in_notify_set(Some(super::NotifyFlags {
            list,
            status,
            metadata,
        }));
    }
    if matches!(cmd, Command::NotifyNone) {
        state.set_in_notify_set(Some(super::NotifyFlags::default()));
    }

    // Encode, tag, and send the command (handles literal sync).
    // Encoding / send errors: the command bytes may not have reached the
    // server, so these are Unsent. The `?` propagates without further
    // decoration; send_command_on_wire owns the pre-send phase.
    let tag = send_command_on_wire(wire_reader, state, tag_gen, event_sink, &cmd).await?;

    // After a successful send, any transport failure is InFlight: the
    // command bytes crossed the side-effect boundary.
    loop {
        let notify_before = state.notify();
        let utf8 = utf8_mode(state);
        consumer
            .prepare_to_read()
            .await
            .map_err(|e| e.with_attempt(TransmissionState::InFlight))?;
        let resp = wire_reader
            .read_one(utf8)
            .await
            .map_err(|e| e.with_attempt(TransmissionState::InFlight))?;
        let _digest = state.apply_side_effects(&resp);

        match resp {
            crate::types::Response::Tagged(t) if t.tag == tag => {
                // (I13): emit alert/notification overflow from
                // tagged response codes before finalization.
                emit_tagged_response_code_events(&t, event_sink);
                let ctx = build_consumer_context(state, cmd_target.as_ref(), &tag);
                // Tagged response received: the server acknowledged the
                // command. Errors from finalization (NO/BAD status) carry
                // Acknowledged so recovery can distinguish them from
                // in-flight drops.
                let finalized = consumer
                    .finalize_erased(t, &ctx)
                    .map_err(|e| e.with_attempt(TransmissionState::Acknowledged))?;
                // Re-emit any responses the consumer marked as events.
                // Skip those whose critical code (ALERT/NOTIFICATIONOVERFLOW)
                // was already emitted in the pre-classification pass
                //  -  re-emitting would double-deliver the alert.
                for resp in finalized.reclassified_as_events {
                    if !has_critical_response_code(&resp) {
                        let _ = event_sink.emit(resp.into());
                    }
                }
                return Ok(finalized.output);
            }
            crate::types::Response::Tagged(t) => {
                return Err(Error::Protocol(format!(
                    "unexpected tag {:?} (expected {:?})",
                    t.tag, tag,
                )));
            }
            crate::types::Response::Untagged(u) => {
                // (I13): emit alert/notification overflow before
                // classification so they reach the event queue even
                // when the response is routed to a consumer.
                let code_emitted = emit_untagged_response_code_events(&u, event_sink);

                let class_ctx = ClassificationContext {
                    notify: notify_before,
                    command_target: cmd_target.as_ref(),
                };
                let rule = classification::classify(cmd_kind, &u, &class_ctx);
                match rule {
                    SolicitationRule::OnlySolicited | SolicitationRule::Either => {
                        let ctx = build_consumer_context(state, cmd_target.as_ref(), &tag);
                        consumer.on_response(*u, notify_before, &ctx);
                    }
                    SolicitationRule::OnlyUnsolicited | SolicitationRule::Impossible => {
                        // Skip event_sink.emit for responses whose
                        // critical content (ALERT / NOTIFICATIONOVERFLOW)
                        // was already emitted above  -  avoid double-emit.
                        if !code_emitted {
                            let _ = event_sink.emit((*u).into());
                        }
                    }
                }
            }
            crate::types::Response::Continuation(c) => {
                // Route to the consumer's continuation handler if
                // supported (RFC 3501 Section7.5). Regular consumers error.
                let ctx = build_consumer_context(state, cmd_target.as_ref(), &tag);
                let ContinuationReply::Write(bytes) = consumer.on_continuation(c, &ctx)?;
                wire_reader
                    .write_all(&bytes)
                    .await
                    .map_err(|e| e.with_attempt(TransmissionState::InFlight))?;
            }
            crate::types::Response::Greeting(_) => {
                return Err(Error::Protocol("unexpected greeting mid-command".into()));
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Pre-built command execution (APPEND / MULTIAPPEND)
// ---------------------------------------------------------------------------

/// Execute a pre-built command whose wire bytes were constructed by the
/// handle side.
///
/// Sends the pre-built bytes using [`send_with_literal_sync`] (which
/// handles synchronizing literal boundaries per RFC 3501 Section4.3), then
/// runs the same classification-based response loop as
/// [`run_one_command`].
///
/// APPEND and MULTIAPPEND use this path because their literal encoding
/// is handled by the handle side rather than by [`encode_command`].
#[allow(clippy::too_many_arguments)]
pub(in crate::connection) async fn run_prebuilt_command(
    wire_reader: &mut super::wire::WireReader,
    state: &mut super::state::ProtocolState,
    event_sink: &mut event_sink::DriverEventSink,
    wire_bytes: BytesMut,
    tag: &str,
    cmd_kind: crate::types::CommandKind,
    cmd_target: Option<MailboxName>,
    mut consumer: DriverConsumer,
) -> Result<Box<dyn std::any::Any + Send>, Error> {
    trace!(tag, ?cmd_kind, "driver: sending pre-built command");

    // Send pre-built bytes with literal synchronization.
    // Errors before a tagged response are Unsent or InFlight depending
    // on where in the send they occur; send_with_literal_sync propagates
    // them without decoration. After send, responses are InFlight.
    send_with_literal_sync(wire_reader, state, event_sink, &wire_bytes).await?;

    // Response classification loop  -  identical to run_one_command.
    loop {
        let notify_before = state.notify();
        let utf8 = utf8_mode(state);
        consumer
            .prepare_to_read()
            .await
            .map_err(|e| e.with_attempt(TransmissionState::InFlight))?;
        let resp = wire_reader
            .read_one(utf8)
            .await
            .map_err(|e| e.with_attempt(TransmissionState::InFlight))?;
        let _digest = state.apply_side_effects(&resp);

        match resp {
            crate::types::Response::Tagged(t) if t.tag == tag => {
                emit_tagged_response_code_events(&t, event_sink);
                let ctx = build_consumer_context(state, cmd_target.as_ref(), tag);
                let finalized = consumer
                    .finalize_erased(t, &ctx)
                    .map_err(|e| e.with_attempt(TransmissionState::Acknowledged))?;
                for resp in finalized.reclassified_as_events {
                    if !has_critical_response_code(&resp) {
                        let _ = event_sink.emit(resp.into());
                    }
                }
                return Ok(finalized.output);
            }
            crate::types::Response::Tagged(t) => {
                return Err(Error::Protocol(format!(
                    "unexpected tag {:?} (expected {:?})",
                    t.tag, tag,
                )));
            }
            crate::types::Response::Untagged(u) => {
                let code_emitted = emit_untagged_response_code_events(&u, event_sink);

                let class_ctx = ClassificationContext {
                    notify: notify_before,
                    command_target: cmd_target.as_ref(),
                };
                let rule = classification::classify(cmd_kind, &u, &class_ctx);
                match rule {
                    SolicitationRule::OnlySolicited | SolicitationRule::Either => {
                        let ctx = build_consumer_context(state, cmd_target.as_ref(), tag);
                        consumer.on_response(*u, notify_before, &ctx);
                    }
                    SolicitationRule::OnlyUnsolicited | SolicitationRule::Impossible => {
                        if !code_emitted {
                            let _ = event_sink.emit((*u).into());
                        }
                    }
                }
            }
            crate::types::Response::Continuation(_) => {
                // APPEND/MULTIAPPEND should not receive continuations
                // after all bytes have been sent. Error per RFC 3501 Section7.5.
                return Err(Error::Protocol(
                    "unexpected continuation after pre-built command fully sent".into(),
                ));
            }
            crate::types::Response::Greeting(_) => {
                return Err(Error::Protocol("unexpected greeting mid-command".into()));
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Pure helpers  -  derive protocol context from ProtocolState
// ---------------------------------------------------------------------------

/// Derive whether the connection is in `IMAP4rev2` mode.
///
/// RFC 9051 Section6.3.1: on dual-mode servers (both rev1 and rev2), rev2
/// behavior requires explicit ENABLE.
fn is_rev2(state: &super::state::ProtocolState) -> bool {
    let has_rev2 = state.capabilities().contains(&Capability::Imap4Rev2);
    let has_rev1 = state.capabilities().contains(&Capability::Imap4Rev1);
    if has_rev2 && has_rev1 {
        state
            .enabled()
            .iter()
            .any(|e| e.eq_ignore_ascii_case("IMAP4rev2"))
    } else {
        has_rev2
    }
}

/// Derive the UTF-8 wire mode from protocol state.
///
/// True when `UTF8=ACCEPT` (RFC 6855 Section3) has been enabled or `IMAP4rev2`
/// is active (RFC 9051 Section7).
fn utf8_mode(state: &super::state::ProtocolState) -> bool {
    state
        .enabled()
        .iter()
        .any(|e| e.eq_ignore_ascii_case("UTF8=ACCEPT"))
        || is_rev2(state)
}

/// Derive the literal negotiation mode from capabilities.
///
/// RFC 7888 Section4 (LITERAL+), Section5 (LITERAL-), RFC 3501 Section4.3 (synchronizing).
fn literal_mode(state: &super::state::ProtocolState) -> LiteralMode {
    if state.capabilities().contains(&Capability::LiteralPlus) {
        LiteralMode::LiteralPlus
    } else if state.capabilities().contains(&Capability::LiteralMinus) || is_rev2(state) {
        LiteralMode::LiteralMinus
    } else {
        LiteralMode::Synchronizing
    }
}

/// Build an [`EncodeOptions`] snapshot from protocol state.
///
/// Mirrors `ImapConnection::encode_options`.
fn build_encode_options(state: &super::state::ProtocolState) -> EncodeOptions {
    EncodeOptions {
        utf8_mode: utf8_mode(state),
        literal_mode: literal_mode(state),
        capabilities: state.capabilities().to_vec(),
        enabled: state.enabled().to_vec(),
    }
}

/// Build a [`ConsumerContext`] from protocol state.
///
/// Mirrors `ImapConnection::build_consumer_context`. The driver
/// constructs this inline instead of going through a method on
/// `ImapConnection`.
fn build_consumer_context<'a>(
    state: &'a super::state::ProtocolState,
    command_target: Option<&'a MailboxName>,
    command_tag: &'a str,
) -> ConsumerContext<'a> {
    ConsumerContext {
        capabilities: state.capabilities(),
        enabled: state.enabled(),
        command_target,
        command_tag,
    }
}

// ---------------------------------------------------------------------------
// Submodules
// ---------------------------------------------------------------------------

pub(super) mod event_sink;

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
