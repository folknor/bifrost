use bytes::BytesMut;
use tracing::trace;

use crate::codec::classification::{self, ClassificationContext, SolicitationRule};
use crate::codec::encode::{LiteralMode, encode_command};
use crate::error::Error;
use crate::types::Command;
use crate::types::response::Capability;
use crate::types::validated::MailboxName;

use super::event_sink;
use super::wire_send::{send_encoded_segments, send_with_literal_sync};
use super::{ConsumerErased, PipelineResults};

/// A sub-batch entry: `(original_index, command, consumer)`.
pub(super) type SubBatchEntry = (usize, Command, Box<dyn ConsumerErased>);

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
pub(super) fn group_into_sub_batches(
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

    for (original_idx, (cmd, consumer)) in commands.into_iter().zip(consumers).enumerate() {
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

/// Everything the pipeline response router needs that is not the protocol
/// state or the event sink: the per-command tables built in step 2, the
/// consumer/result slots, and where in the batch we currently are.
///
/// This exists so the router can run from BOTH read loops - the step 4
/// response loop and the synchronizing-literal continuation wait inside the
/// send phase - rather than the send phase falling back to the consumerless
/// arm. A response read during a literal negotiation belongs to whichever
/// earlier pipelined command solicited it, and only a routing context makes
/// that reachable from there.
pub(super) struct PipelineRouting<'a> {
    /// Index of the command whose synchronizing literal is currently being
    /// negotiated, or `None` in the step 4 response loop.
    ///
    /// Load-bearing twice. It names the command whose OWN tagged response
    /// ends the negotiation, and it is the EXCLUSIVE upper bound on untagged
    /// ownership: a solicited untagged response is one the server produced by
    /// EXECUTING a command, and the server has not received this command (it
    /// is waiting on a literal it has not been given), so it cannot have
    /// executed it and no solicited response for it can exist. The only
    /// things it can legitimately produce for a half-received command are the
    /// continuation and a tagged rejection of the prefix, and both are
    /// matched before classification ever runs. Commands after it have not
    /// been sent at all.
    sending: Option<usize>,
    tag_to_idx: &'a std::collections::HashMap<String, usize>,
    commands: &'a [Command],
    kinds: &'a [crate::types::CommandKind],
    targets: &'a [Option<MailboxName>],
    tags: &'a [String],
    consumers: &'a mut [Option<Box<dyn ConsumerErased>>],
    results: &'a mut [Option<Result<Box<dyn std::any::Any + Send>, Error>>],
    completed: &'a mut usize,
}

impl PipelineRouting<'_> {
    /// The exclusive bound on consumers eligible to own an untagged response.
    fn untagged_bound(&self) -> usize {
        self.sending.unwrap_or(self.consumers.len())
    }
}

/// What a routed response means to the loop that read it.
pub(super) enum Routed {
    /// A `+` continuation. Grants the literal in the send phase; a protocol
    /// error in the step 4 loop.
    Continuation,
    /// The tagged `NO`/`BAD` (or an early `OK`, recorded as `ProtocolMissing`)
    /// of the command currently negotiating its literal. Already finalized
    /// into its own result slot; the sender must
    /// abandon the remainder of that command's bytes.
    OwnTagRejected,
    /// Routed; keep reading.
    Continue,
}

/// Apply the pre-`apply_side_effects` state mutation a tagged completion
/// implies for its own command.
///
/// Among pipelinable commands only NOTIFY SET/NONE need this (RFC 5465
/// Section3). The ordering is load-bearing: `apply_side_effects` consumes the
/// registration this installs, so installing it afterwards loses the
/// transition permanently. It lives here, called once from the router, because
/// the router is reached from two read loops and a second copy of this rule is
/// the drift shape this crate has paid for before.
///
/// Design note: the other state-changing commands (ENABLE, SELECT, CLOSE,
/// UNSELECT) are excluded from pipeline execution by API design - no pipeline
/// methods exist for them - which is why there is no `set_in_close` here.
fn apply_pipeline_pre_effects(
    state: &mut super::super::state::ProtocolState,
    routing: &PipelineRouting<'_>,
    t: &crate::types::response::TaggedResponse,
) {
    let Some(&idx) = routing.tag_to_idx.get(&t.tag) else {
        return;
    };
    match routing.commands[idx] {
        Command::NotifySet(ref params) => {
            let (list, status, metadata) = super::super::extensions::compute_notify_flags(params);
            state.set_in_notify_set(Some(super::super::NotifyFlags {
                list,
                status,
                metadata,
            }));
        }
        Command::NotifyNone => {
            state.set_in_notify_set(Some(super::super::NotifyFlags::default()));
        }
        _ => {}
    }
}

/// Where a tag-correlated ESEARCH goes when its correlator names a search
/// command of this batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CorrelatedEsearch {
    /// The named search is still active: deliver to its consumer.
    Deliver(usize),
    /// The named search has already finalized: this is surplus solicited
    /// data for a completed command, and it is dropped.
    DropSurplus,
}

/// The command a tag-correlated ESEARCH names, when that command is a search
/// of this batch that can have solicited it.
///
/// `Some` only when the response is an ESEARCH with a search-correlator, the
/// tag is one of this batch's commands, and it is a search command for which
/// `classify` makes ESEARCH solicited. Then:
///
/// * the command has finalized (the tag-completion barrier) - `DropSurplus`.
///   Its answer, if it had one, was already taken, so this is surplus
///   solicited data for a completed command, and it gets the same answer
///   `SearchConsumer` gives surplus SEARCH/ESEARCH: dropped, not published.
///   `classify` makes ESEARCH `OnlySolicited` inside a search and
///   `Impossible` anywhere else, so it has no life as an asynchronous event,
///   and the correlator says outright whose it is. Before this arm, head
///   routing handed it to whichever command was still active, which either
///   buffered it as a foreign-tagged ESEARCH and surrendered it, or had no use
///   for it and forwarded it as an event - either way, an event the
///   classifier says cannot exist. A finalized command is always below the
///   untagged-ownership bound or is the rejected sending command, and the
///   late data is surplus in both cases, so the bound is not consulted.
/// * the command is active and below the untagged-ownership bound (the
///   server has received it) - `Deliver`.
///
/// Anything else - a tagless ESEARCH, a tag from outside the batch, a tag
/// naming an unsent command, or a tag naming a non-search command - is
/// `None` and takes the ordinary head-consumer path unchanged.
fn correlated_esearch_owner(
    routing: &PipelineRouting<'_>,
    resp: &crate::types::response::UntaggedResponse,
    bound: usize,
    notify: super::super::NotifyFlags,
) -> Option<CorrelatedEsearch> {
    let crate::types::response::UntaggedResponse::Esearch(e) = resp else {
        return None;
    };
    let idx = *routing.tag_to_idx.get(e.tag.as_deref()?)?;
    let ctx = ClassificationContext {
        notify,
        command_target: routing.targets[idx].as_ref(),
    };
    if !matches!(
        classification::classify(routing.kinds[idx], resp, &ctx),
        SolicitationRule::OnlySolicited
    ) {
        return None;
    }
    if routing.consumers[idx].is_none() {
        return Some(CorrelatedEsearch::DropSurplus);
    }
    (idx < bound).then_some(CorrelatedEsearch::Deliver(idx))
}

/// Route one response read in pipeline context: apply its side effects
/// exactly once, then deliver it to the command that owns it.
///
/// The single router for both pipeline read loops. Side effects are applied
/// here and nowhere else on this path, so a response cannot be processed
/// twice - which is what makes routing a response at READ time correct where
/// buffering it for later replay was not: a replayed `* 3 EXPUNGE` would
/// either decrement twice or need its whole read-time context reconstructed.
#[allow(clippy::too_many_lines)]
pub(super) fn route_pipeline_response(
    state: &mut super::super::state::ProtocolState,
    event_sink: &mut event_sink::DriverEventSink,
    routing: &mut PipelineRouting<'_>,
    resp: crate::types::Response,
) -> Result<Routed, Error> {
    // Read before any mutation: classification is against the notification
    // registration as it stood when this response was produced.
    let notify_before = state.notify();

    // The sending command's own tagged OK before its `+`: the command did not
    // execute, so its pre-effects (a NOTIFY registration) must not install.
    let early_own_ok = matches!(
        resp,
        crate::types::Response::Tagged(ref t)
            if t.status == crate::types::response::StatusKind::Ok
                && routing.sending.is_some()
                && routing.sending == routing.tag_to_idx.get(&t.tag).copied()
    );

    if !early_own_ok && let crate::types::Response::Tagged(ref t) = resp {
        apply_pipeline_pre_effects(state, routing, t);
    }

    let digest = state.apply_side_effects(&resp);

    match resp {
        crate::types::Response::Continuation(_) => Ok(Routed::Continuation),
        crate::types::Response::Greeting(_) => {
            Err(Error::Protocol("unexpected greeting mid-pipeline".into()))
        }
        crate::types::Response::Tagged(t) => {
            // Before every exit below, including the protocol errors: an
            // [ALERT] on a tagged response reaches the event queue whatever
            // happens to the response itself (RFC 3501 Section7.1).
            super::emit_tagged_response_code_events(&t, event_sink);

            let Some(&idx) = routing.tag_to_idx.get(&t.tag) else {
                return Err(Error::Protocol(format!(
                    "unknown tag in pipeline response: {:?}",
                    t.tag,
                )));
            };

            if let Some(sending) = routing.sending {
                if idx == sending {
                    // The server answered the command whose literal it has
                    // not been given (RFC 3501 Section4.3).
                    if early_own_ok {
                        // The same server behaviour the single-command wait
                        // treats as a non-fatal `ProtocolMissing`, and it is
                        // equally safe here: the send phase is sequential
                        // whenever a literal is synchronizing, so the only
                        // bytes of this batch on the wire are earlier
                        // commands (complete) and this command's first line.
                        // No later command has been written, so the server
                        // cannot read one as the missing literal, and the
                        // unsent remainder is dropped exactly as after a
                        // `NO`. The consumer is discarded, NOT finalized:
                        // finalizing would turn the OK into a success for a
                        // command that never ran.
                        if routing.consumers[idx].take().is_some() {
                            routing.results[idx] = Some(Err(super::wire_send::early_ok_error()));
                            *routing.completed += 1;
                        }
                        return Ok(Routed::OwnTagRejected);
                    }
                } else if idx > sending {
                    // A completion for a command still unsent: the server
                    // cannot complete what it has not received.
                    return Err(Error::Protocol(format!(
                        "tagged response for unsent pipelined command: {:?}",
                        t.tag,
                    )));
                }
            }

            let own_tag_rejected = routing.sending == Some(idx);

            if let Some(consumer) = routing.consumers[idx].take() {
                let ctx = super::build_consumer_context(
                    state,
                    routing.targets[idx].as_ref(),
                    &routing.tags[idx],
                );
                let finalized = consumer.finalize_erased(t, &ctx);
                // One emission path for both outcomes. These used to be two
                // arms, and the failure arm did not emit at all, so a command
                // that failed destroyed whatever its consumer had buffered.
                for ev in finalized.reclassified_as_events {
                    if !super::has_critical_response_code(&ev) {
                        let _ = event_sink.emit(ev.into());
                    }
                }
                routing.results[idx] = Some(finalized.output);
                *routing.completed += 1;
            }
            // Duplicate tagged response for an already-finalized command:
            // ignore silently (Postel's law).

            if own_tag_rejected {
                Ok(Routed::OwnTagRejected)
            } else {
                Ok(Routed::Continue)
            }
        }
        crate::types::Response::Untagged(u) => {
            // The prologue's BYE short-circuit stays AHEAD of any routing, so
            // an untagged BYE during a pipelined literal send still aborts the
            // batch and loses every result. That was considered and declined,
            // not overlooked: a BYE means the connection is closing, and
            // handing back partial results while tearing down is a more
            // confusing contract than failing. Changing it means
            // `run_pipeline_batch` returning `(PipelineResults, Option<Error>)`,
            // which ripples into the sub-batch loop and the driver's fatal
            // check; re-raise it only with that whole shape in hand.
            // The router runs only once some of the batch is on the wire, and
            // a BYE completes none of it: InFlight.
            let code_emitted = super::process_untagged_prefix(
                digest,
                &u,
                event_sink,
                bifrost_types::TransmissionState::InFlight,
            )?;
            let bound = routing.untagged_bound();

            // An ESEARCH carrying a search-correlator names its command
            // outright (RFC 4466 search-correlator, RFC 4731 Section 3.1), so it
            // goes to that command rather than to whichever search happens to
            // be the head. Head routing would hand it to an EARLIER search's
            // consumer, which buffers a foreign-tagged ESEARCH and surrenders
            // it as an event, while the command it answers finalizes with no
            // result: a spurious "OK but no ESEARCH" protocol error for a
            // server that merely interleaved two pipelined searches.
            // One correlated to a search that has already finalized is
            // surplus data for a completed command and is dropped.
            match correlated_esearch_owner(routing, &u, bound, notify_before) {
                Some(CorrelatedEsearch::Deliver(owner)) => {
                    let ctx = super::build_consumer_context(
                        state,
                        routing.targets[owner].as_ref(),
                        &routing.tags[owner],
                    );
                    if let Some(ref mut consumer) = routing.consumers[owner] {
                        consumer.on_response(*u, notify_before, &ctx);
                    }
                    return Ok(Routed::Continue);
                }
                Some(CorrelatedEsearch::DropSurplus) => {
                    trace!("driver: dropping surplus ESEARCH for a finalized pipelined search");
                    return Ok(Routed::Continue);
                }
                None => {}
            }

            // Head consumer: the first still-active command eligible to own
            // an untagged response. Per the tag-completion barrier a
            // finalized command can no longer receive one, and per `sending`
            // neither can a command the server has not finished receiving.
            let head_idx = routing.consumers[..bound].iter().position(Option::is_some);
            let Some(idx) = head_idx else {
                // Nothing eligible: late-flushed server data.
                if !code_emitted {
                    let _ = event_sink.emit((*u).into());
                }
                return Ok(Routed::Continue);
            };

            let class_ctx = ClassificationContext {
                notify: notify_before,
                command_target: routing.targets[idx].as_ref(),
            };
            match classification::classify(routing.kinds[idx], &u, &class_ctx) {
                SolicitationRule::OnlySolicited | SolicitationRule::Either => {
                    let ctx = super::build_consumer_context(
                        state,
                        routing.targets[idx].as_ref(),
                        &routing.tags[idx],
                    );
                    if let Some(ref mut consumer) = routing.consumers[idx] {
                        consumer.on_response(*u, notify_before, &ctx);
                    }
                }
                SolicitationRule::OnlyUnsolicited | SolicitationRule::Impossible => {
                    // Forward-classify: the head consumer does not want this
                    // response. Scan later eligible consumers to see if it is
                    // OnlySolicited for one of them. Non-conformant servers
                    // may interleave responses across pipelined commands
                    // (Postel's law).
                    let mut u = Some(u);
                    for later in (idx + 1)..bound {
                        if routing.consumers[later].is_none() {
                            continue;
                        }
                        let claimed = match u {
                            Some(ref inner) => {
                                let later_ctx = ClassificationContext {
                                    notify: notify_before,
                                    command_target: routing.targets[later].as_ref(),
                                };
                                matches!(
                                    classification::classify(
                                        routing.kinds[later],
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
                                let ctx = super::build_consumer_context(
                                    state,
                                    routing.targets[later].as_ref(),
                                    &routing.tags[later],
                                );
                                if let Some(ref mut consumer) = routing.consumers[later] {
                                    consumer.on_response(*taken, notify_before, &ctx);
                                }
                            }
                            break;
                        }
                    }
                    // No eligible consumer claimed it: emit as event.
                    if let Some(unclaimed) = u
                        && !code_emitted
                    {
                        let _ = event_sink.emit((*unclaimed).into());
                    }
                }
            }
            Ok(Routed::Continue)
        }
    }
}

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
pub(super) async fn run_pipeline(
    wire_reader: &mut super::super::wire::WireReader,
    state: &mut super::super::state::ProtocolState,
    tag_gen: &mut super::super::tag::TagGenerator,
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
        // Fast path: all kinds unique, single batch is safe.
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
/// 1. Snapshots encode options (C7 fix: capability state at batch start).
/// 2. Encodes all commands before sending any bytes, and builds the routing
///    tables. Any encode failure aborts the entire batch.
/// 3. Sends all commands on the wire (batch write for LITERAL+ mode).
/// 4. Reads responses, routing each to the correct consumer by tag.
///    Untagged responses are classified against the head (first
///    non-finalized) consumer's command kind. Once a consumer is
///    finalized (its tagged response arrived), it can no longer receive
///    untagged responses: the tag-completion barrier.
///
/// The routing tables are built BEFORE step 3 rather than after it because
/// the send phase reads from the socket too. Whenever a synchronizing literal
/// forces a continuation wait (no LITERAL+, or a LITERAL- literal too large to
/// patch), commands already on the wire have tags still pending, and their
/// responses arrive during that wait. Routing them needs the same tables step
/// 4 uses, so both loops share one router and the send phase is not a
/// consumerless loop.
///
/// Continuations (`+`) are errors in the step 4 loop. Pipelinable commands do
/// not produce continuations of their own (the `Pipelinable` sealed trait
/// enforces this at the type level); a `+` during the send phase is the
/// literal grant and belongs to the sender.
#[allow(clippy::too_many_lines)]
async fn run_pipeline_batch(
    wire_reader: &mut super::super::wire::WireReader,
    state: &mut super::super::state::ProtocolState,
    tag_gen: &mut super::super::tag::TagGenerator,
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
    let opts = super::build_encode_options(state);
    let allow_literal8 =
        state.capabilities().contains(&Capability::Binary) && !super::is_rev2(state);

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

    // Routing tables, built before the send: see the doc comment.
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

    trace!(count, "driver: sending pipelined batch");

    // 3. Send all commands on the wire. For LITERAL+ mode, batch all
    //    into a single buffer for a single-write send. For other modes,
    //    send each command individually with literal synchronization as
    //    needed (RFC 3501 Section4.3).
    match opts.literal_mode {
        LiteralMode::LiteralPlus => {
            // RFC 7888 Section4: all literals are non-synchronizing. Batch
            // everything into a single write.
            let bufs: Vec<BytesMut> = encoded_commands
                .into_iter()
                .map(|e| {
                    let flat = e.into_buf();
                    super::super::patch_literals_to_plus_with_binary(&flat, allow_literal8)
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
            // RFC 7888 Section5: small literals (<=4096) are non-synchronizing;
            // larger ones need sync. Send each command with patching.
            for (i, encoded) in encoded_commands.into_iter().enumerate() {
                let flat = encoded.into_buf();
                let patched =
                    super::super::patch_small_literals_to_plus_with_binary(&flat, allow_literal8);
                let mut routing = PipelineRouting {
                    sending: Some(i),
                    tag_to_idx: &tag_to_idx,
                    commands: &commands,
                    kinds: &kinds,
                    targets: &targets,
                    tags: &tags,
                    consumers: &mut consumers,
                    results: &mut results,
                    completed: &mut completed,
                };
                send_with_literal_sync(
                    wire_reader,
                    state,
                    event_sink,
                    &patched,
                    &tags[i],
                    Some(&mut routing),
                )
                .await?;
            }
        }
        LiteralMode::Synchronizing => {
            // RFC 3501 Section4.3: all literals are synchronizing. Send each
            // command's segments with literal sync.
            for (i, encoded) in encoded_commands.into_iter().enumerate() {
                let mut routing = PipelineRouting {
                    sending: Some(i),
                    tag_to_idx: &tag_to_idx,
                    commands: &commands,
                    kinds: &kinds,
                    targets: &targets,
                    tags: &tags,
                    consumers: &mut consumers,
                    results: &mut results,
                    completed: &mut completed,
                };
                send_encoded_segments(
                    wire_reader,
                    state,
                    event_sink,
                    encoded.segments(),
                    &tags[i],
                    Some(&mut routing),
                )
                .await?;
            }
        }
    }

    // 4. Response loop with tag-completion barrier.
    //
    // The send phase may already have routed responses - and, when the last
    // command's literal was rejected, may already have finalized every
    // command - so the guard is checked before the first read rather than
    // after it.
    while completed < count {
        let utf8 = super::utf8_mode(state);
        let resp = wire_reader.read_one(utf8).await?;
        let mut routing = PipelineRouting {
            sending: None,
            tag_to_idx: &tag_to_idx,
            commands: &commands,
            kinds: &kinds,
            targets: &targets,
            tags: &tags,
            consumers: &mut consumers,
            results: &mut results,
            completed: &mut completed,
        };
        match route_pipeline_response(state, event_sink, &mut routing, resp)? {
            Routed::Continuation => {
                // Pipelinable commands do not produce continuations
                // (enforced by the Pipelinable sealed trait).
                // An unexpected + is a protocol error (RFC 3501 Section7.5).
                return Err(Error::Protocol(
                    "unexpected continuation in pipeline response loop".into(),
                ));
            }
            // Unreachable with `sending: None`: no tag is the own tag.
            Routed::OwnTagRejected | Routed::Continue => {}
        }
    }

    // Collect results in command order. Every entry should be Some after
    // the loop: the while guard ensures `completed == count`.
    Ok(results
        .into_iter()
        .map(|r| r.unwrap_or_else(|| Err(Error::Internal("missing pipeline result".into()))))
        .collect())
}

#[cfg(test)]
#[path = "pipeline_tests.rs"]
mod tests;
