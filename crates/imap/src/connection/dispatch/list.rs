use crate::connection::NotifyFlags;
use crate::connection::helpers::inbox_eq;
use crate::error::Error;
use crate::types::response::{TaggedResponse, UntaggedResponse};
use crate::types::validated::MailboxName;
use crate::types::{MailboxInfo, StatusItem, StatusResult};

use super::super::{is_notify_list_event, is_notify_selection_mismatch};
use super::{Consumer, ConsumerContext, Finalized};

/// Consumer for LIST (RFC 3501 Section6.3.8).
///
/// Accumulates solicited LIST responses and classifies NOTIFY marker-
/// bearing LIST responses (OLDNAME, `\NonExistent`, `\NoAccess`) as
/// events to be re-emitted by the dispatcher (RFC 5465 Section5.4).
///
/// The `notify_snapshot` parameter on each `on_response` call provides
/// the per-response NOTIFY state. After a mid-stream
/// `[NOTIFICATIONOVERFLOW]`, `apply_side_effects` clears the notify
/// flags, so subsequent snapshots have `list = false`. This replaces
/// the manual `first_notification_overflow_index` approach used by the
/// old hand-rolled loop.
///
/// `Output` is `Result<Vec<MailboxInfo>, Error>` so that NOTIFY marker
/// events can be reclassified even when the tagged response is NO/BAD.
pub(crate) struct ListConsumer {
    /// Solicited LIST entries (marker-less, accumulated on success).
    mailboxes: Vec<MailboxInfo>,
    /// NOTIFY marker-bearing LIST entries. Reclassified as events in
    /// `finalize` regardless of tagged status.
    marker_events: Vec<UntaggedResponse>,
    /// Non-LIST responses routed here via `Either` classification.
    buffered: Vec<UntaggedResponse>,
}

impl ListConsumer {
    pub(crate) fn new() -> Self {
        Self {
            mailboxes: Vec::new(),
            marker_events: Vec::new(),
            buffered: Vec::new(),
        }
    }
}

impl Consumer for ListConsumer {
    type Output = Result<Vec<MailboxInfo>, Error>;

    fn on_response(
        &mut self,
        resp: UntaggedResponse,
        notify_snapshot: NotifyFlags,
        _ctx: &ConsumerContext,
    ) {
        match resp {
            UntaggedResponse::List(info) => {
                // RFC 5465 Section5.4: when NOTIFY LIST events are registered,
                // marker-bearing LIST responses are NOTIFY events. The
                // per-response notify_snapshot handles mid-stream overflow
                // (RFC 5465 Section5.8): after overflow, snapshot.list is false.
                if notify_snapshot.list && is_notify_list_event(&info, true) {
                    self.marker_events.push(UntaggedResponse::List(info));
                } else {
                    self.mailboxes.push(info);
                }
            }
            other => self.buffered.push(other),
        }
    }

    fn finalize(
        self: Box<Self>,
        tagged: TaggedResponse,
        _ctx: &ConsumerContext,
    ) -> Result<Finalized<Result<Vec<MailboxInfo>, Error>>, Error> {
        // Marker events are reclassified as events on both success and
        // failure paths. Non-LIST buffered responses are also reclassified.
        let mut reclassified = self.marker_events;
        reclassified.extend(self.buffered);

        match tagged.require_ok() {
            Ok(_) => Ok(Finalized {
                output: Ok(self.mailboxes),
                reclassified_as_events: reclassified,
            }),
            Err(e) => {
                // On failure: marker-less LIST may be the failed solicited
                // result. Drop it rather than leaking as a notification
                // (RFC 5465 Section5.4). Marker events are still emitted.
                Ok(Finalized {
                    output: Err(e),
                    reclassified_as_events: reclassified,
                })
            }
        }
    }
}

/// Consumer for LSUB (RFC 3501 Section6.3.9).
///
/// Simple accumulator. LSUB has no NOTIFY ambiguity (deprecated in
/// `IMAP4rev2`; RFC 9051 Appendix F item 19). All LSUB responses
/// classified as `OnlySolicited` are accumulated; any `Either` responses
/// are reclassified as events.
#[derive(Default)]
pub(crate) struct LsubConsumer {
    mailboxes: Vec<MailboxInfo>,
    buffered: Vec<UntaggedResponse>,
}

impl Consumer for LsubConsumer {
    type Output = Vec<MailboxInfo>;

    fn on_response(
        &mut self,
        resp: UntaggedResponse,
        _notify_snapshot: NotifyFlags,
        _ctx: &ConsumerContext,
    ) {
        match resp {
            UntaggedResponse::Lsub(info) => {
                self.mailboxes.push(info);
            }
            other => self.buffered.push(other),
        }
    }

    fn finalize(
        self: Box<Self>,
        tagged: TaggedResponse,
        _ctx: &ConsumerContext,
    ) -> Result<Finalized<Vec<MailboxInfo>>, Error> {
        tagged.require_ok()?;
        Ok(Finalized {
            output: self.mailboxes,
            reclassified_as_events: self.buffered,
        })
    }
}

/// Consumer for LIST-EXTENDED (RFC 5258 Section3 / RFC 9051 Section6.3.9).
///
/// Like [`ListConsumer`] but additionally filters selection-mismatch
/// NOTIFY events (RFC 5258 Section3: responses that lack the required
/// `\Subscribed`, `\Remote`, or special-use attributes).
///
/// `filter_extended` controls whether `\NonExistent` / `\NoAccess` are
/// treated as NOTIFY markers. When `SUBSCRIBED` is in the selection
/// options, these attributes are legitimate solicited data (RFC 5258 Section3)
/// and must NOT be filtered.
pub(crate) struct ListExtendedConsumer {
    /// Whether to treat `\NonExistent` / `\NoAccess` as NOTIFY markers.
    /// `true` when SUBSCRIBED is NOT in selection options.
    filter_extended: bool,
    /// Selection options for mismatch detection (owned copies).
    selection_options: Vec<String>,
    /// Solicited LIST entries.
    mailboxes: Vec<MailboxInfo>,
    /// NOTIFY marker-bearing LIST entries.
    marker_events: Vec<UntaggedResponse>,
    /// Selection-mismatch NOTIFY events (already decoded and pushed
    /// directly to reclassified, not through `buffer_remaining`).
    mismatch_events: Vec<UntaggedResponse>,
    /// Non-LIST responses routed here via `Either`.
    buffered: Vec<UntaggedResponse>,
}

impl ListExtendedConsumer {
    pub(crate) fn new(filter_extended: bool, selection_options: Vec<String>) -> Self {
        Self {
            filter_extended,
            selection_options,
            mailboxes: Vec::new(),
            marker_events: Vec::new(),
            mismatch_events: Vec::new(),
            buffered: Vec::new(),
        }
    }
}

impl Consumer for ListExtendedConsumer {
    type Output = Result<Vec<MailboxInfo>, Error>;

    fn on_response(
        &mut self,
        resp: UntaggedResponse,
        notify_snapshot: NotifyFlags,
        _ctx: &ConsumerContext,
    ) {
        match resp {
            UntaggedResponse::List(info) => {
                if notify_snapshot.list {
                    // RFC 5465 Section5.4: check for NOTIFY marker events.
                    if is_notify_list_event(&info, self.filter_extended) {
                        self.marker_events.push(UntaggedResponse::List(info));
                        return;
                    }
                    // RFC 5258 Section3: check selection-option mismatch. Build
                    // a temporary &[&str] view for the helper function.
                    let opts: Vec<&str> =
                        self.selection_options.iter().map(String::as_str).collect();
                    if is_notify_selection_mismatch(&info, &opts) {
                        self.mismatch_events.push(UntaggedResponse::List(info));
                        return;
                    }
                }
                self.mailboxes.push(info);
            }
            other => self.buffered.push(other),
        }
    }

    fn finalize(
        self: Box<Self>,
        tagged: TaggedResponse,
        _ctx: &ConsumerContext,
    ) -> Result<Finalized<Result<Vec<MailboxInfo>, Error>>, Error> {
        // Marker events and mismatch events are reclassified on both
        // success and failure paths. They are provably NOTIFY events
        // and must not be dropped.
        let mut reclassified = self.marker_events;
        reclassified.extend(self.mismatch_events);
        reclassified.extend(self.buffered);

        match tagged.require_ok() {
            Ok(_) => Ok(Finalized {
                output: Ok(self.mailboxes),
                reclassified_as_events: reclassified,
            }),
            Err(e) => {
                // On failure: marker-less, non-mismatch LIST may be
                // the failed solicited result. Drop it.
                Ok(Finalized {
                    output: Err(e),
                    reclassified_as_events: reclassified,
                })
            }
        }
    }
}

/// Consumer for LIST with STATUS return option (RFC 5819 Section2).
///
/// Correlates interleaved LIST and STATUS responses by mailbox name.
/// Both LIST and STATUS are classified as `OnlySolicited` during
/// LIST-STATUS (see `classify`). NOTIFY marker-bearing LIST entries
/// are identified via `is_notify_list_event` and reclassified.
///
/// On failure: marker-bearing LIST -> reclassified as events; all STATUS
/// and marker-less LIST -> dropped (STATUS is wire-identical to NOTIFY,
/// RFC 5465 Section4 / RFC 5819 Section2).
pub(crate) struct ListStatusConsumer {
    /// Accumulated solicited LIST entries paired with their STATUS data.
    /// STATUS slot is `None` until the correlated STATUS arrives.
    results: Vec<(MailboxInfo, Option<Vec<StatusItem>>)>,
    /// STATUS responses that arrived before their LIST (valid per
    /// RFC 5819: ordering is not mandated).
    pending_status: Vec<(MailboxName, Vec<StatusItem>)>,
    /// NOTIFY marker-bearing LIST entries.
    marker_events: Vec<UntaggedResponse>,
    /// Non-LIST/non-STATUS responses routed here via `Either`.
    buffered: Vec<UntaggedResponse>,
}

impl ListStatusConsumer {
    pub(crate) fn new() -> Self {
        Self {
            results: Vec::new(),
            pending_status: Vec::new(),
            marker_events: Vec::new(),
            buffered: Vec::new(),
        }
    }
}

impl Consumer for ListStatusConsumer {
    type Output = Result<Vec<(MailboxInfo, Vec<StatusItem>)>, Error>;

    fn on_response(
        &mut self,
        resp: UntaggedResponse,
        notify_snapshot: NotifyFlags,
        _ctx: &ConsumerContext,
    ) {
        match resp {
            UntaggedResponse::List(info) => {
                // RFC 5465 Section5.4: NOTIFY marker detection.
                if notify_snapshot.list && is_notify_list_event(&info, true) {
                    self.marker_events.push(UntaggedResponse::List(info));
                    return;
                }
                // Solicited LIST entry, waiting for correlated STATUS.
                self.results.push((info, None));
            }
            UntaggedResponse::MailboxStatus { mailbox, items } => {
                // Positional correlation: pair with the first unpaired
                // LIST for this mailbox. Use `inbox_eq` for
                // case-insensitive INBOX matching (RFC 3501 Section5.1).
                if let Some((_, status)) = self
                    .results
                    .iter_mut()
                    .find(|(mb, s)| s.is_none() && inbox_eq(mb.name.as_str(), mailbox.as_str()))
                {
                    *status = Some(items);
                } else {
                    // STATUS arrived before its LIST. Save for second
                    // pass in finalize (valid LIST-STATUS ordering per
                    // RFC 5819).
                    self.pending_status.push((mailbox, items));
                }
            }
            other => self.buffered.push(other),
        }
    }

    fn finalize(
        self: Box<Self>,
        tagged: TaggedResponse,
        _ctx: &ConsumerContext,
    ) -> Result<Finalized<Result<Vec<(MailboxInfo, Vec<StatusItem>)>, Error>>, Error> {
        // Marker events are reclassified regardless of success/failure.
        let mut reclassified = self.marker_events;
        reclassified.extend(self.buffered);

        if let Err(e) = tagged.require_ok() {
            // On failure: drop all accumulated LIST and STATUS
            // (STATUS is wire-identical to NOTIFY per RFC 5465 Section4).
            // Only NOTIFY marker events survive.
            return Ok(Finalized {
                output: Err(e),
                reclassified_as_events: reclassified,
            });
        }

        // Second pass: pair STATUS that arrived before their LIST.
        let mut results = self.results;
        for (decoded, items) in self.pending_status {
            if let Some((_, status)) = results
                .iter_mut()
                .find(|(mb, s)| s.is_none() && inbox_eq(mb.name.as_str(), decoded.as_str()))
            {
                *status = Some(items);
            }
            // Orphaned STATUS inside LIST-STATUS is ambiguous:
            // could be malformed solicited output or NOTIFY
            // delivery. Drop rather than manufacturing a fake
            // notification (RFC 5465 Section4; RFC 5819 Section2).
        }

        // Replace None with empty vec for any LIST without a
        // matching STATUS (non-conformant server or STATUS not
        // yet arrived). See RFC 5465 Section5.5 / RFC 5819 Section2 for
        // the ambiguity reasoning.
        let paired: Vec<(MailboxInfo, Vec<StatusItem>)> = results
            .into_iter()
            .map(|(mb, status)| {
                let items = status.unwrap_or_default();
                (mb, items)
            })
            .collect();

        Ok(Finalized {
            output: Ok(paired),
            reclassified_as_events: reclassified,
        })
    }
}

/// Consumer for STATUS (RFC 3501 Section6.3.10).
///
/// Accumulates same-mailbox STATUS responses (classified as
/// `OnlySolicited` by `classify`). When NOTIFY STATUS is active
/// (RFC 5465 Section4), additional same-mailbox STATUS responses are
/// ambiguous. The protocol provides no marker to distinguish solicited
/// from NOTIFY. These are surfaced in [`StatusResult::ambiguous`] rather
/// than silently reclassified.
///
/// On failure: all same-mailbox STATUS is dropped. Buffering as
/// unsolicited would leak potentially-solicited data into the event
/// channel (RFC 5465 Section4, RFC 3501 Section6.3.10).
pub(crate) struct StatusConsumer {
    /// Same-mailbox STATUS responses with their per-response notify
    /// snapshot flag. The last entry becomes the primary result;
    /// earlier entries with `had_notify == true` become ambiguous.
    matching: Vec<(UntaggedResponse, bool)>,
    /// Non-STATUS responses routed here via `Either`.
    buffered: Vec<UntaggedResponse>,
}

impl StatusConsumer {
    pub(crate) fn new() -> Self {
        Self {
            matching: Vec::new(),
            buffered: Vec::new(),
        }
    }
}

impl Consumer for StatusConsumer {
    type Output = StatusResult;

    fn on_response(
        &mut self,
        resp: UntaggedResponse,
        notify_snapshot: NotifyFlags,
        _ctx: &ConsumerContext,
    ) {
        match resp {
            UntaggedResponse::MailboxStatus { .. } => {
                // Record whether NOTIFY STATUS was active when this
                // response was generated. Used in finalize to classify
                // extras as ambiguous vs unsolicited.
                self.matching.push((resp, notify_snapshot.status));
            }
            other => self.buffered.push(other),
        }
    }

    fn finalize(
        self: Box<Self>,
        tagged: TaggedResponse,
        ctx: &ConsumerContext,
    ) -> Result<Finalized<StatusResult>, Error> {
        // On failure: drop all matching STATUS AND any Either-classified
        // responses (e.g., * OK [ALERT]). Same pattern as other consumers.
        // Leaking potentially-solicited STATUS as unsolicited events is
        // worse than losing a transient alert (RFC 5465 Section4).
        tagged.require_ok()?;

        let mut matching = self.matching;

        // RFC 3501 Section6.3.10: an OK response MUST include an untagged
        // STATUS for the requested mailbox.
        let Some((last_resp, _)) = matching.pop() else {
            let target = ctx
                .command_target()
                .map_or_else(|| "<unknown>".to_owned(), |t| t.as_str().to_owned());
            return Err(Error::Protocol(format!(
                "STATUS OK but no matching untagged STATUS response \
                 for mailbox '{target}' (RFC 3501 Sections 5.2, 6.3.10)"
            )));
        };

        // Extract the primary items from the last response.
        let UntaggedResponse::MailboxStatus {
            items: primary_items,
            ..
        } = last_resp
        else {
            return Err(Error::Protocol(
                "internal: matching predicate returned non-MailboxStatus \
                 variant"
                    .into(),
            ));
        };

        // RFC 5465 Section4 / Section5.8: classify remaining extras.
        // NOTIFY active -> ambiguous. No NOTIFY -> server anomaly,
        // reclassify as unsolicited per RFC 3501 Section5.2.
        let mut ambiguous = Vec::new();
        let mut non_notify_extras: Vec<UntaggedResponse> = Vec::new();
        for (resp, had_notify) in matching {
            if had_notify {
                if let UntaggedResponse::MailboxStatus { items, .. } = resp {
                    ambiguous.push(items);
                }
            } else {
                non_notify_extras.push(resp);
            }
        }

        let mut reclassified = self.buffered;
        reclassified.extend(non_notify_extras);

        Ok(Finalized {
            output: StatusResult {
                items: primary_items,
                ambiguous,
            },
            reclassified_as_events: reclassified,
        })
    }
}
