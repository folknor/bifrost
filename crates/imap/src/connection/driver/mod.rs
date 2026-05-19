//! Driver task. Owns the `WireReader` and `ProtocolState` exclusively.
//! The public API submits commands over mpsc and awaits oneshot replies.
//!
//! The driver task is the only code that touches the wire reader and
//! protocol state directly. All command execution flows through
//! `run_one_command`, which runs the dispatch loop on decomposed
//! primitives (`WireReader`, `ProtocolState`, tag generator,
//! `DriverEventSink`).

use std::sync::Arc;

use bytes::BytesMut;
use tokio::sync::{mpsc, oneshot, watch};
use tracing::{debug, trace, warn};

use crate::codec::classification::{self, ClassificationContext, SolicitationRule};
use crate::codec::encode::{EncodeOptions, LiteralMode, encode_command};
use crate::error::Error;
use crate::types::Command;
use crate::types::response::{Capability, ResponseCode, StatusKind, UntaggedResponse};
use crate::types::validated::MailboxName;

use super::NotifyFlags;
use super::dispatch::{
    CapabilityConsumer, Consumer, ConsumerContext, ContinuationConsumer, ContinuationReply,
    Finalized, TaggedOkConsumer,
};
use super::typed_event::TypedEvent;

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
            Self::WithContinuations(c) => c.finalize_erased(tagged, ctx),
        }
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
    let tag = send_command_on_wire(wire_reader, state, tag_gen, event_sink, &cmd).await?;

    loop {
        let notify_before = state.notify();
        let utf8 = utf8_mode(state);
        let resp = wire_reader.read_one(utf8).await?;
        let _digest = state.apply_side_effects(&resp);

        match resp {
            crate::types::Response::Tagged(t) if t.tag == tag => {
                // (I13): emit alert/notification overflow from
                // tagged response codes before finalization.
                emit_tagged_response_code_events(&t, event_sink);
                let ctx = build_consumer_context(state, cmd_target.as_ref(), &tag);
                let finalized = consumer.finalize_erased(t, &ctx)?;
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
                wire_reader.write_all(&bytes).await?;
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
    send_with_literal_sync(wire_reader, state, event_sink, &wire_bytes).await?;

    // Response classification loop  -  identical to run_one_command.
    loop {
        let notify_before = state.notify();
        let utf8 = utf8_mode(state);
        let resp = wire_reader.read_one(utf8).await?;
        let _digest = state.apply_side_effects(&resp);

        match resp {
            crate::types::Response::Tagged(t) if t.tag == tag => {
                emit_tagged_response_code_events(&t, event_sink);
                let ctx = build_consumer_context(state, cmd_target.as_ref(), tag);
                let finalized = consumer.finalize_erased(t, &ctx)?;
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
// Pipeline sub-batch grouping
// ---------------------------------------------------------------------------

/// A sub-batch entry: `(original_index, command, consumer)`.
type SubBatchEntry = (usize, Command, Box<dyn ConsumerErased>);

/// Group commands into sub-batches where each sub-batch contains at most
/// one command per [`CommandKind`](crate::types::CommandKind).
///
/// RFC 3501 Section5.5: when multiple commands of the same kind are pipelined,
/// their untagged responses may interleave and the head-consumer routing
/// heuristic cannot disambiguate which consumer owns which response. By
/// splitting duplicate kinds into separate sub-batches executed
/// sequentially, each batch is guaranteed to have unique kinds and
/// head-consumer routing is correct.
///
/// Each entry in the returned sub-batches carries its original index in
/// the input `commands` vec for result reassembly.
fn group_into_sub_batches(
    commands: Vec<Command>,
    consumers: Vec<Box<dyn ConsumerErased>>,
) -> Vec<Vec<SubBatchEntry>> {
    let mut sub_batches: Vec<(
        std::collections::HashSet<crate::types::CommandKind>,
        Vec<SubBatchEntry>,
    )> = vec![(std::collections::HashSet::new(), Vec::new())];

    // Tracks the highest batch index assigned so far. Commands are never
    // placed in a batch before max_batch, preserving their original order
    // across sub-batches.
    let mut max_batch: usize = 0;

    for (original_idx, (cmd, consumer)) in
        commands.into_iter().zip(consumers.into_iter()).enumerate()
    {
        let kind = cmd.kind();
        // Find the first sub-batch at or after max_batch that doesn't
        // already have this kind. The max_batch constraint ensures
        // commands are never placed in a batch before any preceding
        // command, preserving original pipeline order across sub-batches.
        let batch_idx = sub_batches
            .iter()
            .enumerate()
            .skip(max_batch)
            .find_map(|(i, (kinds_seen, _))| (!kinds_seen.contains(&kind)).then_some(i));
        let batch_idx = if let Some(i) = batch_idx {
            i
        } else {
            sub_batches.push((std::collections::HashSet::new(), Vec::new()));
            sub_batches.len() - 1
        };
        sub_batches[batch_idx].0.insert(kind);
        max_batch = batch_idx;
        sub_batches[batch_idx].1.push((original_idx, cmd, consumer));
    }

    sub_batches
        .into_iter()
        .map(|(_, entries)| entries)
        .collect()
}

// ---------------------------------------------------------------------------
// Pipeline execution
// ---------------------------------------------------------------------------

/// Execute a batch of pipelined commands, splitting into sub-batches
/// when duplicate [`CommandKind`]s are present.
///
/// RFC 3501 Section5.5: clients may send multiple commands without waiting for
/// a response, but untagged responses may interleave and the head-consumer
/// routing heuristic cannot disambiguate when two consumers share the same
/// `CommandKind`. This function groups commands into sub-batches where each
/// sub-batch contains at most one command per `CommandKind`, then executes
/// each sub-batch via [`run_pipeline_batch`]. Results are reassembled in
/// original command order.
///
/// When all commands have unique kinds (the common case), only one
/// sub-batch is created and the function delegates directly without
/// grouping overhead.
async fn run_pipeline(
    wire_reader: &mut super::wire::WireReader,
    state: &mut super::state::ProtocolState,
    tag_gen: &mut super::tag::TagGenerator,
    event_sink: &mut event_sink::DriverEventSink,
    commands: Vec<Command>,
    consumers: Vec<Box<dyn ConsumerErased>>,
) -> Result<PipelineResults, Error> {
    let count = commands.len();
    if count == 0 {
        return Ok(Vec::new());
    }

    // Check whether all commands have unique kinds. If so, skip the
    // grouping overhead and delegate directly to run_pipeline_batch.
    let has_duplicates = {
        let mut seen = std::collections::HashSet::with_capacity(count);
        commands.iter().any(|cmd| !seen.insert(cmd.kind()))
    };

    if !has_duplicates {
        // Fast path  -  all kinds unique, single batch is safe.
        return run_pipeline_batch(wire_reader, state, tag_gen, event_sink, commands, consumers)
            .await;
    }

    let sub_batches = group_into_sub_batches(commands, consumers);

    let num_batches = sub_batches.len();
    trace!(
        count,
        num_batches, "driver: splitting pipeline into sub-batches"
    );

    // Pre-allocate results vec indexed by original command position.
    let mut all_results: Vec<Option<Result<Box<dyn std::any::Any + Send>, Error>>> =
        (0..count).map(|_| None).collect();

    // Execute each sub-batch sequentially.
    for entries in sub_batches {
        let original_indices: Vec<usize> = entries.iter().map(|(idx, _, _)| *idx).collect();
        let (batch_cmds, batch_consumers): (Vec<Command>, Vec<Box<dyn ConsumerErased>>) = entries
            .into_iter()
            .map(|(_, cmd, cons)| (cmd, cons))
            .unzip();

        let batch_results = run_pipeline_batch(
            wire_reader,
            state,
            tag_gen,
            event_sink,
            batch_cmds,
            batch_consumers,
        )
        .await?;

        // Place batch results at their original indices.
        for (batch_pos, result) in batch_results.into_iter().enumerate() {
            all_results[original_indices[batch_pos]] = Some(result);
        }
    }

    // Convert Option<Result> to Result. Every entry should be Some
    // after executing all sub-batches.
    Ok(all_results
        .into_iter()
        .map(|r| r.unwrap_or_else(|| Err(Error::Internal("missing pipeline result".into()))))
        .collect())
}

/// Execute a single sub-batch of pipelined commands through the
/// classification-based dispatcher with tag-completion barrier.
///
/// **Precondition**: all commands in the batch have unique
/// [`CommandKind`]s. This is guaranteed by [`run_pipeline`] which splits
/// duplicate kinds into separate sub-batches.
///
/// RFC 3501 Section5.5: clients may send multiple commands without waiting for
/// a response. The server processes them in order, but responses may
/// interleave. This function:
///
/// 1. Snapshots encode options (C7 fix  -  capability state at batch start).
/// 2. Encodes all commands before sending any bytes. Any encode failure
///    aborts the entire batch.
/// 3. Sends all commands on the wire (batch write for LITERAL+ mode).
/// 4. Reads responses, routing each to the correct consumer by tag.
///    Untagged responses are classified against the head (first
///    non-finalized) consumer's command kind. Once a consumer is
///    finalized (its tagged response arrived), it can no longer receive
///    untagged responses  -  the tag-completion barrier.
///
/// Continuations (`+`) are errors in pipeline context  -  pipelinable
/// commands do not produce continuations (the `Pipelinable` sealed trait
/// enforces this at the type level).
#[allow(clippy::too_many_lines)]
async fn run_pipeline_batch(
    wire_reader: &mut super::wire::WireReader,
    state: &mut super::state::ProtocolState,
    tag_gen: &mut super::tag::TagGenerator,
    event_sink: &mut event_sink::DriverEventSink,
    commands: Vec<Command>,
    consumers: Vec<Box<dyn ConsumerErased>>,
) -> Result<PipelineResults, Error> {
    let count = commands.len();
    if count == 0 {
        return Ok(Vec::new());
    }

    // 1. Snapshot encode options at start-of-batch (C7 fix).
    // All commands in the batch are encoded against the same capability
    // state, preventing mid-pipeline staleness.
    let opts = build_encode_options(state);
    let allow_literal8 = state.capabilities().contains(&Capability::Binary) && !is_rev2(state);

    // 2. Encode all commands. Any encode failure aborts the whole batch
    //    before any bytes go on the wire.
    let mut tags: Vec<String> = Vec::with_capacity(count);
    let mut kinds: Vec<crate::types::CommandKind> = Vec::with_capacity(count);
    let mut targets: Vec<Option<MailboxName>> = Vec::with_capacity(count);
    let mut encoded_commands = Vec::with_capacity(count);

    for cmd in &commands {
        let tag = tag_gen.next();
        let kind = cmd.kind();
        let target = cmd.mailbox_target().cloned();
        let encoded = encode_command(&tag, cmd, &opts)?;
        tags.push(tag);
        kinds.push(kind);
        targets.push(target);
        encoded_commands.push(encoded);
    }

    trace!(count, "driver: sending pipelined batch");

    // 3. Send all commands on the wire. For LITERAL+ mode, batch all
    //    into a single buffer for a single-write send. For other modes,
    //    send each command individually with literal synchronization as
    //    needed (RFC 3501 Section4.3).
    match opts.literal_mode {
        LiteralMode::LiteralPlus => {
            // RFC 7888 Section4: all literals are non-synchronizing  -  batch
            // everything into a single write.
            let bufs: Vec<BytesMut> = encoded_commands
                .into_iter()
                .map(|e| {
                    let flat = e.into_buf();
                    super::patch_literals_to_plus_with_binary(&flat, allow_literal8)
                })
                .collect();
            let total: usize = bufs.iter().map(BytesMut::len).sum();
            let mut batch = BytesMut::with_capacity(total);
            for buf in bufs {
                batch.extend_from_slice(&buf);
            }
            wire_reader.write_all(&batch).await?;
        }
        LiteralMode::LiteralMinus => {
            // RFC 7888 Section5: small literals (≤4096) are non-synchronizing;
            // larger ones need sync. Send each command with patching.
            for encoded in encoded_commands {
                let flat = encoded.into_buf();
                let patched =
                    super::patch_small_literals_to_plus_with_binary(&flat, allow_literal8);
                send_with_literal_sync(wire_reader, state, event_sink, &patched).await?;
            }
        }
        LiteralMode::Synchronizing => {
            // RFC 3501 Section4.3: all literals are synchronizing. Send each
            // command's segments with literal sync.
            for encoded in encoded_commands {
                send_encoded_segments(wire_reader, state, event_sink, encoded.segments()).await?;
            }
        }
    }

    // 4. Response loop with tag-completion barrier.
    //
    // Build a tag->index lookup for O(1) matching. Consumers are stored
    // in an Option vec  -  None means finalized.
    let mut tag_to_idx: std::collections::HashMap<String, usize> =
        std::collections::HashMap::with_capacity(count);
    for (i, tag) in tags.iter().enumerate() {
        tag_to_idx.insert(tag.clone(), i);
    }

    let mut consumers: Vec<Option<Box<dyn ConsumerErased>>> =
        consumers.into_iter().map(Some).collect();
    let mut results: Vec<Option<Result<Box<dyn std::any::Any + Send>, Error>>> =
        (0..count).map(|_| None).collect();
    let mut completed = 0usize;

    while completed < count {
        let notify_before = state.notify();
        let utf8 = utf8_mode(state);
        let resp = wire_reader.read_one(utf8).await?;

        // Set pre-flight state mutations before apply_side_effects
        // processes the tagged response. Among pipelinable commands,
        // only NOTIFY SET/NONE need this (RFC 5465 Section3).
        //
        // Design note: State-changing commands (ENABLE, SELECT, CLOSE, UNSELECT)
        // are excluded from pipeline execution by API design  -  no pipeline methods
        // exist for them. This is enforced structurally via the Pipeline type, not
        // by runtime validation. There is no `set_in_close` call here for the
        // same reason.
        if let crate::types::Response::Tagged(ref t) = resp {
            if let Some(&idx) = tag_to_idx.get(&t.tag) {
                match commands[idx] {
                    Command::NotifySet(ref params) => {
                        let (list, status, metadata) =
                            super::extensions::compute_notify_flags(params);
                        state.set_in_notify_set(Some(super::NotifyFlags {
                            list,
                            status,
                            metadata,
                        }));
                    }
                    Command::NotifyNone => {
                        state.set_in_notify_set(Some(super::NotifyFlags::default()));
                    }
                    _ => {}
                }
            }
        }

        let _digest = state.apply_side_effects(&resp);

        match resp {
            crate::types::Response::Tagged(t) => {
                emit_tagged_response_code_events(&t, event_sink);
                if let Some(&idx) = tag_to_idx.get(&t.tag) {
                    if let Some(consumer) = consumers[idx].take() {
                        let ctx = build_consumer_context(state, targets[idx].as_ref(), &tags[idx]);
                        match consumer.finalize_erased(t, &ctx) {
                            Ok(finalized) => {
                                for ev in finalized.reclassified_as_events {
                                    if !has_critical_response_code(&ev) {
                                        let _ = event_sink.emit(ev.into());
                                    }
                                }
                                results[idx] = Some(Ok(finalized.output));
                            }
                            Err(e) => {
                                results[idx] = Some(Err(e));
                            }
                        }
                        completed += 1;
                    }
                    // Duplicate tagged response for already-finalized
                    // command  -  ignore silently (Postel's law).
                } else {
                    return Err(Error::Protocol(format!(
                        "unknown tag in pipeline response: {:?}",
                        t.tag,
                    )));
                }
            }
            crate::types::Response::Untagged(u) => {
                let code_emitted = emit_untagged_response_code_events(&u, event_sink);

                // Find the head consumer  -  first still-active
                // (non-finalized) consumer. Per the tag-completion
                // barrier, only consumers whose tagged response hasn't
                // arrived yet can receive untagged responses.
                let head_idx = consumers.iter().position(Option::is_some);
                if let Some(idx) = head_idx {
                    let class_ctx = ClassificationContext {
                        notify: notify_before,
                        command_target: targets[idx].as_ref(),
                    };
                    let rule = classification::classify(kinds[idx], &u, &class_ctx);
                    match rule {
                        SolicitationRule::OnlySolicited | SolicitationRule::Either => {
                            let ctx =
                                build_consumer_context(state, targets[idx].as_ref(), &tags[idx]);
                            if let Some(ref mut consumer) = consumers[idx] {
                                consumer.on_response(*u, notify_before, &ctx);
                            }
                        }
                        SolicitationRule::OnlyUnsolicited | SolicitationRule::Impossible => {
                            // Forward-classify: the head consumer does not
                            // want this response. Scan later consumers to
                            // see if it is OnlySolicited for one of them  -
                            // non-conformant servers may interleave responses
                            // across pipelined commands (Postel's law).
                            let mut u = Some(u);
                            for later in (idx + 1)..consumers.len() {
                                if consumers[later].is_none() {
                                    continue;
                                }
                                let claimed = match u {
                                    Some(ref inner) => {
                                        let later_ctx = ClassificationContext {
                                            notify: notify_before,
                                            command_target: targets[later].as_ref(),
                                        };
                                        matches!(
                                            classification::classify(
                                                kinds[later],
                                                inner,
                                                &later_ctx,
                                            ),
                                            SolicitationRule::OnlySolicited
                                        )
                                    }
                                    None => false,
                                };
                                if claimed {
                                    if let Some(taken) = u.take() {
                                        let ctx = build_consumer_context(
                                            state,
                                            targets[later].as_ref(),
                                            &tags[later],
                                        );
                                        if let Some(ref mut consumer) = consumers[later] {
                                            consumer.on_response(*taken, notify_before, &ctx);
                                        }
                                    }
                                    break;
                                }
                            }
                            // No later consumer claimed it  -  emit as event.
                            if let Some(unclaimed) = u {
                                if !code_emitted {
                                    let _ = event_sink.emit((*unclaimed).into());
                                }
                            }
                        }
                    }
                } else {
                    // All consumers finalized  -  late-flushed server data.
                    if !code_emitted {
                        let _ = event_sink.emit((*u).into());
                    }
                }
            }
            crate::types::Response::Continuation(_) => {
                // Pipelinable commands do not produce continuations
                // (enforced by the Pipelinable sealed trait).
                // An unexpected + is a protocol error (RFC 3501 Section7.5).
                return Err(Error::Protocol(
                    "unexpected continuation in pipeline response loop".into(),
                ));
            }
            crate::types::Response::Greeting(_) => {
                return Err(Error::Protocol("unexpected greeting mid-pipeline".into()));
            }
        }
    }

    // Collect results in command order. Every entry should be Some after
    // the loop  -  the while guard ensures `completed == count`.
    Ok(results
        .into_iter()
        .map(|r| r.unwrap_or_else(|| Err(Error::Internal("missing pipeline result".into()))))
        .collect())
}

// ---------------------------------------------------------------------------
// IDLE session (RFC 2177)
// ---------------------------------------------------------------------------

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
async fn run_idle(
    wire_reader: &mut super::wire::WireReader,
    state: &mut super::state::ProtocolState,
    tag_gen: &mut super::tag::TagGenerator,
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
            result = wire_reader.read_one(utf8_mode(state)) => {
                let resp = result?;
                let digest = state.apply_side_effects(&resp);
                match resp {
                    crate::types::Response::Tagged(t) if t.tag == tag => {
                        // RFC 2177 Section 3: server terminated IDLE.
                        emit_tagged_response_code_events(&t, event_sink);
                        // Check status  -  NO/BAD is an error, not a
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
                        return Err(Error::Protocol(format!(
                            "unexpected tag {:?} during IDLE (expected {:?})",
                            t.tag, tag,
                        )));
                    }
                    crate::types::Response::Untagged(u) => {
                        let code_emitted =
                            emit_untagged_response_code_events(&u, event_sink);
                        // RFC 3501 Section7.1.5: BYE means the server is
                        // closing. State already transitioned to Logout
                        // by apply_side_effects.
                        if digest.had_bye {
                            let (text, code) = match *u {
                                UntaggedResponse::Status { text, code, .. } => {
                                    (text, code)
                                }
                                _ => (String::new(), None),
                            };
                            warn!(text, "received BYE during IDLE");
                            return Err(Error::bye_with_code(text, code));
                        }
                        if !code_emitted {
                            let _ = event_sink.emit((*u).into());
                        }
                    }
                    crate::types::Response::Continuation(_) => {
                        // Unexpected continuation during IDLE  -
                        // Postel's law: ignore and continue reading.
                        debug!(tag, "ignoring unexpected continuation during IDLE");
                    }
                    crate::types::Response::Greeting(_) => {
                        return Err(Error::Protocol(
                            "unexpected greeting during IDLE".into(),
                        ));
                    }
                }
                false
            }
        };

        if done_signaled {
            // 4. Client requested DONE (RFC 2177 Section 3).
            trace!(tag, "driver: sending DONE");
            wire_reader.write_all(b"DONE\r\n").await?;
            drain_idle_responses(wire_reader, state, event_sink, &tag).await?;
            trace!(tag, "driver: exited IDLE mode");
            return Ok(IdleTermination::ClientDone);
        }
    }
}

/// Drain remaining responses after DONE until the tagged OK arrives
/// (RFC 2177 Section 3).
///
/// The server may send additional untagged responses between the
/// client's DONE and the tagged OK. These are emitted as events.
async fn drain_idle_responses(
    wire_reader: &mut super::wire::WireReader,
    state: &mut super::state::ProtocolState,
    event_sink: &mut event_sink::DriverEventSink,
    tag: &str,
) -> Result<(), Error> {
    loop {
        let utf8 = utf8_mode(state);
        let resp = wire_reader.read_one(utf8).await?;
        let digest = state.apply_side_effects(&resp);
        match resp {
            crate::types::Response::Tagged(t) if t.tag == tag => {
                emit_tagged_response_code_events(&t, event_sink);
                // Check status  -  NO/BAD after DONE is an error.
                match t.status {
                    StatusKind::Ok => return Ok(()),
                    StatusKind::No => return Err(Error::no_with_code(t.text, t.code)),
                    StatusKind::Bad => return Err(Error::bad_with_code(t.text, t.code)),
                }
            }
            crate::types::Response::Tagged(t) => {
                return Err(Error::Protocol(format!(
                    "unexpected tag {:?} during IDLE drain (expected {:?})",
                    t.tag, tag,
                )));
            }
            crate::types::Response::Untagged(u) => {
                let code_emitted = emit_untagged_response_code_events(&u, event_sink);
                // RFC 3501 Section7.1.5: BYE during IDLE drain.
                if digest.had_bye {
                    let (text, code) = match *u {
                        UntaggedResponse::Status { text, code, .. } => (text, code),
                        _ => (String::new(), None),
                    };
                    warn!(text, "received BYE during IDLE drain");
                    return Err(Error::bye_with_code(text, code));
                }
                if !code_emitted {
                    let _ = event_sink.emit((*u).into());
                }
            }
            crate::types::Response::Continuation(_) | crate::types::Response::Greeting(_) => {
                // Postel's law: ignore unexpected continuations/greetings
                // during IDLE drain.
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Command sending with literal synchronization
// ---------------------------------------------------------------------------

/// Encode and send a command over the wire, handling literal
/// synchronization (RFC 3501 Section4.3).
///
/// Returns the generated tag on success. Mirrors
/// `ImapConnection::send_command` adapted for the driver's
/// decomposed primitives.
async fn send_command_on_wire(
    wire_reader: &mut super::wire::WireReader,
    state: &mut super::state::ProtocolState,
    tag_gen: &mut super::tag::TagGenerator,
    event_sink: &mut event_sink::DriverEventSink,
    cmd: &Command,
) -> Result<String, Error> {
    let tag = tag_gen.next();
    trace!(tag, ?cmd, "driver: sending IMAP command");
    let opts = build_encode_options(state);
    let encoded = encode_command(&tag, cmd, &opts)?;

    let allow_literal8 = state.capabilities().contains(&Capability::Binary) && !is_rev2(state);

    match opts.literal_mode {
        LiteralMode::LiteralPlus => {
            // LITERAL+ path (RFC 7888 Section4): patch all synchronizing
            // markers to non-synchronizing and send in one shot.
            let flat = encoded.into_buf();
            let patched = super::patch_literals_to_plus_with_binary(&flat, allow_literal8);
            send_with_literal_sync(wire_reader, state, event_sink, &patched).await?;
        }
        LiteralMode::LiteralMinus => {
            // LITERAL- path (RFC 7888 Section5): patch small (≤4096 byte)
            // literals to non-synchronizing; larger ones stay sync.
            let flat = encoded.into_buf();
            let patched = super::patch_small_literals_to_plus_with_binary(&flat, allow_literal8);
            send_with_literal_sync(wire_reader, state, event_sink, &patched).await?;
        }
        LiteralMode::Synchronizing => {
            // No literal extension  -  all literals are synchronizing
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
async fn send_with_literal_sync(
    wire_reader: &mut super::wire::WireReader,
    state: &mut super::state::ProtocolState,
    event_sink: &mut event_sink::DriverEventSink,
    buf: &[u8],
) -> Result<(), Error> {
    let mut pos = 0;
    while pos < buf.len() {
        if let Some((marker_end, literal_size)) = super::find_literal_boundary(&buf[pos..]) {
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
            // No more literals  -  send the rest.
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
async fn send_encoded_segments(
    wire_reader: &mut super::wire::WireReader,
    state: &mut super::state::ProtocolState,
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
async fn wait_for_continuation(
    wire_reader: &mut super::wire::WireReader,
    state: &mut super::state::ProtocolState,
    event_sink: &mut event_sink::DriverEventSink,
) -> Result<(), Error> {
    loop {
        let utf8 = utf8_mode(state);
        let resp = wire_reader.read_one(utf8).await?;
        let digest = state.apply_side_effects(&resp);
        match resp {
            crate::types::Response::Continuation(_) => return Ok(()),
            crate::types::Response::Tagged(t) => {
                // Server rejected the command before the literal was
                // sent. Side effects already applied (RFC 3501 Section4.3).
                emit_tagged_response_code_events(&t, event_sink);
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
                let code_emitted = emit_untagged_response_code_events(&u, event_sink);

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

// ---------------------------------------------------------------------------
// Stream upgrades  -  STARTTLS and COMPRESS
// ---------------------------------------------------------------------------

/// Execute a stream upgrade atomically.
///
/// Dispatches to the appropriate upgrade handler based on the payload.
/// The driver runs the protocol command internally (using a
/// `TaggedOkConsumer`), then atomically swaps the stream using the
/// `Poisoned` sentinel (I9, I10).
async fn run_upgrade(
    wire_reader: &mut super::wire::WireReader,
    state: &mut super::state::ProtocolState,
    tag_gen: &mut super::tag::TagGenerator,
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
/// 2. Verify the wire buffer is empty (B10 fix  -  no injected bytes).
/// 3. `mem::replace` the reader with a `Poisoned`-stream reader. No
///    `.await` between the buffer check and the replace.
/// 4. TLS handshake (may suspend). If the handshake fails or the
///    future is cancelled, the reader stays wrapping `Poisoned`
///    forever and the connection is dead (I9).
/// 5. Install a fresh `WireReader` on the new TLS stream. The fresh
///    reader has an empty buffer  -  the old buffer was dropped with
///    the old reader in step 3 (I10).
/// 6. Re-fetch capabilities (RFC 3501 Section6.2.1).
pub(in crate::connection) async fn run_starttls_upgrade(
    wire_reader: &mut super::wire::WireReader,
    state: &mut super::state::ProtocolState,
    tag_gen: &mut super::tag::TagGenerator,
    event_sink: &mut event_sink::DriverEventSink,
    tls_connector: native_tls::TlsConnector,
    server_name: String,
) -> Result<(), Error> {
    // Step 1: Send STARTTLS command, await tagged OK (RFC 3501 Section6.2.1).
    let consumer =
        DriverConsumer::Regular(Box::new(TaggedOkConsumer::default()) as Box<dyn ConsumerErased>);
    run_one_command(
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
        *wire_reader = super::wire::WireReader::new(super::ImapStream::Poisoned);
        state.apply_infrastructure_failure();
        return Err(Error::Protocol(
            "STARTTLS: unexpected bytes in buffer at upgrade boundary \
             (possible MITM  -  RFC 3501 Section 6.2.1)"
                .into(),
        ));
    }

    // Step 3: Atomic swap  -  replace the reader with one on a Poisoned
    // stream. The old reader is consumed  -  its buffer is dropped  -  and
    // we get the old stream back (I10).
    let old_reader = std::mem::replace(
        wire_reader,
        super::wire::WireReader::new(super::ImapStream::Poisoned),
    );
    let old_stream = old_reader.into_stream();
    let Some(tcp) = old_stream.into_tcp() else {
        // Should be unreachable  -  the handle validates the stream
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
            // TLS handshake failed  -  connection is dead (Poisoned stays).
            state.apply_infrastructure_failure();
            return Err(Error::Io(Arc::new(std::io::Error::other(e))));
        }
    };

    // Step 5: Install a fresh WireReader on the new TLS stream.
    // The reader has a fresh empty buffer  -  the old buffer was
    // dropped with old_reader in Step 3 (I10).
    *wire_reader = super::wire::WireReader::new(super::ImapStream::Tls(tls_stream));

    // Step 6: Re-read capabilities after TLS upgrade (RFC 3501 Section6.2.1).
    state.apply_capability_fetch(Vec::new());
    let cap_consumer =
        DriverConsumer::Regular(Box::new(CapabilityConsumer::default()) as Box<dyn ConsumerErased>);
    let result = run_one_command(
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
    wire_reader: &mut super::wire::WireReader,
    state: &mut super::state::ProtocolState,
    tag_gen: &mut super::tag::TagGenerator,
    event_sink: &mut event_sink::DriverEventSink,
) -> Result<(), Error> {
    // Step 1: Send COMPRESS command, await tagged OK (RFC 4978 Section4).
    let consumer =
        DriverConsumer::Regular(Box::new(TaggedOkConsumer::default()) as Box<dyn ConsumerErased>);
    run_one_command(
        wire_reader,
        state,
        tag_gen,
        event_sink,
        Command::Compress,
        consumer,
    )
    .await?;

    // Step 2: Take remaining buffer bytes  -  they are compressed data
    // that must be preserved in the new CompressedStream's raw read
    // buffer (RFC 4978 Section3: server begins compressing immediately
    // after the CRLF ending the tagged OK).
    let remaining = wire_reader.take_buffer();

    // Step 3: Atomic swap with Poisoned sentinel.
    let old_reader = std::mem::replace(
        wire_reader,
        super::wire::WireReader::new(super::ImapStream::Poisoned),
    );
    let old_stream = old_reader.into_stream();

    // Step 4: Wrap the old stream in a CompressedStream.
    // After the mem::replace above, Poisoned is installed. If any of
    // these error paths fire, the connection is dead  -  transition state
    // to Logout so `require_state` rejects subsequent commands cleanly.
    let inner = match old_stream {
        super::ImapStream::Plain(tcp) => super::InnerStream::Plain(tcp),
        super::ImapStream::Tls(tls) => super::InnerStream::Tls(tls),
        super::ImapStream::Compressed(_) => {
            state.apply_infrastructure_failure();
            return Err(Error::Protocol(
                "COMPRESS=DEFLATE already active on this connection".into(),
            ));
        }
        super::ImapStream::Poisoned => {
            state.apply_infrastructure_failure();
            return Err(Error::Protocol(
                "stream poisoned  -  connection is dead".into(),
            ));
        }
        #[cfg(test)]
        super::ImapStream::Memory(_) => {
            state.apply_infrastructure_failure();
            return Err(Error::Protocol(
                "COMPRESS=DEFLATE not supported on in-memory test streams".into(),
            ));
        }
    };

    // Step 5: Build the CompressedStream and install.
    // RFC 4978 Section3: the server begins compressing immediately after the
    // CRLF ending the tagged OK.
    let mut compressed = super::CompressedStream::new(inner);
    if !remaining.is_empty() {
        compressed.raw_read_buf.extend_from_slice(&remaining);
    }
    *wire_reader = super::wire::WireReader::new(super::ImapStream::Compressed(compressed));

    debug!("COMPRESS=DEFLATE activated (RFC 4978)");
    Ok(())
}

// ---------------------------------------------------------------------------
// Graceful shutdown
// ---------------------------------------------------------------------------

/// Best-effort LOGOUT on graceful shutdown (RFC 3501 Section6.1.3).
///
/// Sends LOGOUT and reads the BYE/OK response. Errors are ignored  -
/// the connection is being torn down and the caller has already
/// dropped `cmd_tx`.
async fn logout_best_effort(
    wire_reader: &mut super::wire::WireReader,
    state: &mut super::state::ProtocolState,
    tag_gen: &mut super::tag::TagGenerator,
) -> Result<(), Error> {
    if state.session_state() == super::SessionState::Logout {
        return Ok(());
    }
    let tag = tag_gen.next();
    // LOGOUT is a trivial command with no literals  -  write raw bytes.
    let logout_line = format!("{tag} LOGOUT\r\n");
    wire_reader.write_all(logout_line.as_bytes()).await?;

    // Read responses until the tagged OK or an error. Apply side
    // effects so state transitions to Logout on the BYE/tagged OK.
    loop {
        let utf8 = utf8_mode(state);
        let resp = wire_reader.read_one(utf8).await?;
        let _digest = state.apply_side_effects(&resp);
        match resp {
            crate::types::Response::Tagged(t) if t.tag == tag => break,
            crate::types::Response::Tagged(_) => break,
            _ => {}
        }
    }
    Ok(())
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
// Response-code event emission (I13)
// ---------------------------------------------------------------------------

/// Emit [`TypedEvent::Alert`] or [`TypedEvent::NotificationOverflow`]
/// from an untagged response's response code, if present.
///
/// RFC 3501 Section7.1: `[ALERT]` response codes MUST be presented to the
/// user. RFC 5465 Section5.8: `[NOTIFICATIONOVERFLOW]` means the NOTIFY
/// registration was dropped. Both must reach the event queue regardless
/// of how the response is classified (solicited, unsolicited, or
/// impossible). The driver calls this *before* classification so the
/// event is emitted even when the response is routed to a consumer.
///
/// Returns `true` if a critical event was extracted (caller may skip
/// the redundant `From<UntaggedResponse>` conversion to avoid
/// double-emitting the same data).
fn emit_untagged_response_code_events(
    u: &UntaggedResponse,
    event_sink: &mut event_sink::DriverEventSink,
) -> bool {
    match u {
        UntaggedResponse::Status {
            code: Some(ResponseCode::Alert),
            text,
            ..
        } => {
            let _ = event_sink.emit(TypedEvent::Alert(text.clone()));
            true
        }
        UntaggedResponse::Status {
            code: Some(ResponseCode::NotificationOverflow(detail)),
            text,
            ..
        } => {
            let _ = event_sink.emit(TypedEvent::NotificationOverflow {
                code: detail.clone(),
                text: text.clone(),
            });
            true
        }
        _ => false,
    }
}

/// Emit [`TypedEvent::Alert`] or [`TypedEvent::NotificationOverflow`]
/// from a tagged response's response code, if present.
///
/// Tagged responses go to `consumer.finalize_erased()` and never reach
/// the `From<UntaggedResponse>` event conversion. Without this
/// explicit emission, `[ALERT]` and `[NOTIFICATIONOVERFLOW]` in tagged
/// responses would be captured by `apply_side_effects` (state mutation)
/// but never published as typed events (bug B7, I13).
fn emit_tagged_response_code_events(
    t: &crate::types::response::TaggedResponse,
    event_sink: &mut event_sink::DriverEventSink,
) {
    match &t.code {
        Some(ResponseCode::Alert) => {
            let _ = event_sink.emit(TypedEvent::Alert(t.text.clone()));
        }
        Some(ResponseCode::NotificationOverflow(detail)) => {
            let _ = event_sink.emit(TypedEvent::NotificationOverflow {
                code: detail.clone(),
                text: t.text.clone(),
            });
        }
        _ => {}
    }
}

/// Check whether an untagged response carries `[ALERT]` or
/// `[NOTIFICATIONOVERFLOW]`  -  response codes that were already emitted
/// as typed events in the pre-classification pass.
///
/// Used by the reclassified-as-events loop: if a consumer reclassifies
/// a response that was already emitted by `emit_untagged_response_code_events`,
/// the reclassified copy must be skipped to prevent double-delivery.
fn has_critical_response_code(u: &UntaggedResponse) -> bool {
    matches!(
        u,
        UntaggedResponse::Status {
            status,
            code: Some(ResponseCode::Alert),
            ..
        } | UntaggedResponse::Status {
            status,
            code: Some(ResponseCode::NotificationOverflow(_)),
            ..
        }
        if !matches!(status, crate::types::response::UntaggedStatus::Bye)
    )
}

pub(super) mod event_sink;

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
