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
/// 2. Encodes all commands before sending any bytes. Any encode failure
///    aborts the entire batch.
/// 3. Sends all commands on the wire (batch write for LITERAL+ mode).
/// 4. Reads responses, routing each to the correct consumer by tag.
///    Untagged responses are classified against the head (first
///    non-finalized) consumer's command kind. Once a consumer is
///    finalized (its tagged response arrived), it can no longer receive
///    untagged responses: the tag-completion barrier.
///
/// Continuations (`+`) are errors in pipeline context. Pipelinable
/// commands do not produce continuations (the `Pipelinable` sealed trait
/// enforces this at the type level).
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
            for encoded in encoded_commands {
                let flat = encoded.into_buf();
                let patched =
                    super::super::patch_small_literals_to_plus_with_binary(&flat, allow_literal8);
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
    // in an Option vec: None means finalized.
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
        let utf8 = super::utf8_mode(state);
        let resp = wire_reader.read_one(utf8).await?;

        // Set pre-flight state mutations before apply_side_effects
        // processes the tagged response. Among pipelinable commands,
        // only NOTIFY SET/NONE need this (RFC 5465 Section3).
        //
        // Design note: State-changing commands (ENABLE, SELECT, CLOSE, UNSELECT)
        // are excluded from pipeline execution by API design: no pipeline methods
        // exist for them. This is enforced structurally via the Pipeline type, not
        // by runtime validation. There is no `set_in_close` call here for the
        // same reason.
        if let crate::types::Response::Tagged(ref t) = resp
            && let Some(&idx) = tag_to_idx.get(&t.tag)
        {
            match commands[idx] {
                Command::NotifySet(ref params) => {
                    let (list, status, metadata) =
                        super::super::extensions::compute_notify_flags(params);
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

        let digest = state.apply_side_effects(&resp);

        match resp {
            crate::types::Response::Tagged(t) => {
                super::emit_tagged_response_code_events(&t, event_sink);
                if let Some(&idx) = tag_to_idx.get(&t.tag) {
                    if let Some(consumer) = consumers[idx].take() {
                        let ctx =
                            super::build_consumer_context(state, targets[idx].as_ref(), &tags[idx]);
                        match consumer.finalize_erased(t, &ctx) {
                            Ok(finalized) => {
                                for ev in finalized.reclassified_as_events {
                                    if !super::has_critical_response_code(&ev) {
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
                    // command: ignore silently (Postel's law).
                } else {
                    return Err(Error::Protocol(format!(
                        "unknown tag in pipeline response: {:?}",
                        t.tag,
                    )));
                }
            }
            crate::types::Response::Untagged(u) => {
                let code_emitted = super::emit_untagged_response_code_events(&u, event_sink);
                super::short_circuit_on_bye(digest, &u)?;

                // Find the head consumer: first still-active
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
                            let ctx = super::build_consumer_context(
                                state,
                                targets[idx].as_ref(),
                                &tags[idx],
                            );
                            if let Some(ref mut consumer) = consumers[idx] {
                                consumer.on_response(*u, notify_before, &ctx);
                            }
                        }
                        SolicitationRule::OnlyUnsolicited | SolicitationRule::Impossible => {
                            // Forward-classify: the head consumer does not
                            // want this response. Scan later consumers to
                            // see if it is OnlySolicited for one of them.
                            // Non-conformant servers may interleave responses
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
                                        let ctx = super::build_consumer_context(
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
                            // No later consumer claimed it: emit as event.
                            if let Some(unclaimed) = u
                                && !code_emitted
                            {
                                let _ = event_sink.emit((*unclaimed).into());
                            }
                        }
                    }
                } else {
                    // All consumers finalized: late-flushed server data.
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
    // the loop: the while guard ensures `completed == count`.
    Ok(results
        .into_iter()
        .map(|r| r.unwrap_or_else(|| Err(Error::Internal("missing pipeline result".into()))))
        .collect())
}
