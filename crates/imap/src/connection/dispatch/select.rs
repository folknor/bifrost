use crate::connection::NotifyFlags;
use crate::connection::helpers::inbox_eq;
use crate::error::Error;
use crate::types::SelectedMailbox;
use crate::types::response::{
    Capability, ResponseCode, StatusKind, TaggedResponse, UntaggedResponse,
};

use super::super::{
    build_selected_mailbox, is_notify_list_event, selected_mailbox_effective_responses,
};
use super::{Consumer, ConsumerContext, Finalized};

/// Consumer for SELECT (RFC 3501 Section6.3.1) and EXAMINE (RFC 3501 Section6.3.2).
///
/// Accumulates the mandatory untagged response sequence (EXISTS, RECENT,
/// FLAGS) plus optional response codes (UIDVALIDITY, UIDNEXT,
/// PERMANENTFLAGS, HIGHESTMODSEQ, NOMODSEQ, UNSEEN, MAILBOXID,
/// UIDNOTSTICKY) and QRESYNC data (VANISHED EARLIER, FETCH with changed
/// flags  -  RFC 7162 Section3.2.5.2).
///
/// Unlike [`FetchVanishedConsumer`], this consumer does **not** filter
/// `VANISHED (EARLIER)` responses against the `known-uids` set.
/// RFC 7162 Section 3.2.5.2: during SELECT/EXAMINE with QRESYNC,
/// `known-uids` is a server hint for optimization, not a scoping
/// constraint  -  the server may legitimately return expunged UIDs outside
/// the known set based on its own `seq-match-data` computation.
///
/// `Output` is `Result<SelectedMailbox, Error>` rather than
/// `SelectedMailbox` so that NO / BAD / validation-failure paths can
/// still reclassify accumulated responses as events (the outer
/// `Finalized` always succeeds). The connection method unwraps the
/// inner `Result` for the caller.
pub(crate) struct SelectConsumer {
    /// Whether this is EXAMINE (always read-only) or SELECT.
    is_examine: bool,
    /// All responses delivered by the dispatcher. Partitioned in `finalize`
    /// on the `[CLOSED]` boundary (RFC 7162 Section3.2.11).
    responses: Vec<UntaggedResponse>,
}

impl SelectConsumer {
    pub(crate) fn new(is_examine: bool) -> Self {
        Self {
            is_examine,
            responses: Vec::new(),
        }
    }
}

impl Consumer for SelectConsumer {
    type Output = Result<SelectedMailbox, Error>;

    fn on_response(
        &mut self,
        resp: UntaggedResponse,
        _notify_snapshot: NotifyFlags,
        _ctx: &ConsumerContext,
    ) {
        // Accumulate everything. Partitioning on [CLOSED] and filtering
        // non-SELECT types happens in finalize.
        self.responses.push(resp);
    }

    fn finalize(
        self: Box<Self>,
        tagged: TaggedResponse,
        ctx: &ConsumerContext,
    ) -> Result<Finalized<Result<SelectedMailbox, Error>>, Error> {
        match tagged.status {
            // NO / BAD: reclassify all accumulated responses as events.
            // They may be legitimate unsolicited updates for the previously
            // selected mailbox (RFC 3501 Section7).
            StatusKind::No => Ok(Finalized {
                output: Err(Error::no_with_code(tagged.text, tagged.code)),
                reclassified_as_events: self.responses,
            }),
            StatusKind::Bad => Ok(Finalized {
                output: Err(Error::bad_with_code(tagged.text, tagged.code)),
                reclassified_as_events: self.responses,
            }),
            StatusKind::Ok => {
                let read_only = if self.is_examine {
                    true
                } else {
                    // RFC 3501 Section6.3.1: [READ-ONLY] in the tagged OK means the
                    // mailbox was opened read-only despite being SELECT'd.
                    tagged.code.as_ref() == Some(&ResponseCode::ReadOnly)
                };

                // Validate mandatory responses (RFC 3501 Section6.3.1-6.3.2).
                // Only post-[CLOSED] responses count; pre-CLOSED belong to
                // the previously selected mailbox (RFC 7162 Section3.2.11).
                let effective = selected_mailbox_effective_responses(&self.responses);
                if let Err(e) = validate_select_responses(effective, self.is_examine, ctx) {
                    // Validation failed. Reclassify everything as events so
                    // legitimate unsolicited updates are not lost.
                    return Ok(Finalized {
                        output: Err(e),
                        reclassified_as_events: self.responses,
                    });
                }

                // Build the SelectedMailbox from accumulated responses. The
                // helper internally handles the [CLOSED] boundary.
                let result = build_selected_mailbox(&self.responses, &tagged, read_only);

                // Partition for reclassification. Pre-CLOSED responses are
                // old-mailbox data -> events. Post-CLOSED non-SELECT types
                // and NOTIFY-marked LIST are async notifications -> events.
                let reclassified = reclassify_select_responses(self.responses, ctx);

                Ok(Finalized {
                    output: Ok(result),
                    reclassified_as_events: reclassified,
                })
            }
        }
    }
}

/// Partition responses into events after a successful SELECT/EXAMINE.
///
/// Pre-`[CLOSED]` responses are old-mailbox data and always reclassified.
/// Post-`[CLOSED]` responses are split: SELECT-solicited types (EXISTS,
/// RECENT, FLAGS, VANISHED, FETCH, status codes, and the solicited rev2
/// LIST) are consumed by [`build_selected_mailbox`]; everything else
/// (EXPUNGE, NOTIFY-marked LIST, etc.) is reclassified as events.
fn reclassify_select_responses(
    responses: Vec<UntaggedResponse>,
    ctx: &ConsumerContext,
) -> Vec<UntaggedResponse> {
    let closed_idx = responses.iter().rposition(|r| {
        matches!(
            r,
            UntaggedResponse::Status {
                code: Some(ResponseCode::Closed),
                ..
            }
        )
    });

    let mut reclassified = Vec::new();

    // Track whether the solicited rev2 LIST has been consumed (at most one).
    let mut consumed_select_list = false;

    match closed_idx {
        Some(idx) => {
            let mut owned = responses;
            let post = owned.split_off(idx + 1);
            // Drop the CLOSED marker itself (last element of pre-split).
            owned.pop();
            // Pre-CLOSED: all go to events (old-mailbox notifications).
            reclassified = owned;
            // Post-CLOSED: non-SELECT types go to events.
            for r in post {
                if !is_select_solicited_response(&r, ctx, &mut consumed_select_list) {
                    reclassified.push(r);
                }
            }
        }
        None => {
            for r in responses {
                if !is_select_solicited_response(&r, ctx, &mut consumed_select_list) {
                    reclassified.push(r);
                }
            }
        }
    }

    reclassified
}

/// Check whether a response is one of the types solicited by SELECT/EXAMINE.
///
/// Used to partition post-`[CLOSED]` responses: SELECT types are consumed
/// by [`build_selected_mailbox`]; everything else is reclassified as an
/// event.
///
/// For LIST: only the first unmarked LIST matching the command target is
/// consumed as the mandatory rev2 response (RFC 9051 Section6.3.2). NOTIFY-
/// marked LIST responses (OLDNAME, `\NonExistent`, `\NoAccess`) are
/// always reclassified as events.
///
/// Note: `Vanished { earlier: false }` is consumed here even though
/// `build_selected_mailbox` only extracts `earlier: true`. Non-earlier
/// VANISHED during SELECT is rare (an asynchronous expunge for the new
/// mailbox arriving before the tagged OK) and is silently consumed,
/// consistent with the pre-dispatcher implementation.
fn is_select_solicited_response(
    resp: &UntaggedResponse,
    ctx: &ConsumerContext,
    consumed_select_list: &mut bool,
) -> bool {
    match resp {
        UntaggedResponse::Exists(_)
        | UntaggedResponse::Recent(_)
        | UntaggedResponse::Flags(_)
        | UntaggedResponse::Vanished { .. }
        | UntaggedResponse::Fetch(_)
        | UntaggedResponse::Status { code: Some(_), .. } => true,
        // RFC 9051 Section6.3.2: rev2 SELECT solicits exactly one LIST for
        // the selected mailbox. Consume the first unmarked LIST
        // matching the command target; reclassify NOTIFY-marked LIST.
        UntaggedResponse::List(info) => {
            if *consumed_select_list {
                return false;
            }
            if let Some(target) = ctx.command_target()
                && inbox_eq(target.as_str(), info.name.as_str())
                && !is_notify_list_event(info, true)
            {
                *consumed_select_list = true;
                return true;
            }
            false
        }
        _ => false,
    }
}

/// Validate that the mandatory SELECT/EXAMINE responses are present
/// (RFC 3501 Section6.3.1-6.3.2, RFC 9051 Section6.3.2-6.3.3).
///
/// For `IMAP4rev1`: FLAGS, EXISTS, and RECENT are required.
/// For `IMAP4rev2`: FLAGS, EXISTS, and a matching LIST are required.
fn validate_select_responses(
    effective: &[UntaggedResponse],
    is_examine: bool,
    ctx: &ConsumerContext,
) -> Result<(), Error> {
    let is_rev2 = {
        let has_rev2 = ctx.capabilities().contains(&Capability::Imap4Rev2);
        let has_rev1 = ctx.capabilities().contains(&Capability::Imap4Rev1);
        if has_rev2 && has_rev1 {
            // RFC 9051 Section6.3.1: dual-mode requires ENABLE IMAP4REV2.
            ctx.enabled()
                .iter()
                .any(|e| e.eq_ignore_ascii_case("IMAP4REV2"))
        } else {
            has_rev2
        }
    };

    let command_name = if is_examine { "EXAMINE" } else { "SELECT" };
    let section = match (is_rev2, is_examine) {
        (true, false) => "RFC 9051 Section 6.3.2",
        (true, true) => "RFC 9051 Section 6.3.3",
        (false, false) => "RFC 3501 Section 6.3.1",
        (false, true) => "RFC 3501 Section 6.3.2",
    };

    let mut saw_flags = false;
    let mut saw_exists = false;
    let mut saw_recent = false;
    let mut saw_list = false;

    for resp in effective {
        match resp {
            UntaggedResponse::Flags(_) => saw_flags = true,
            UntaggedResponse::Exists(_) => saw_exists = true,
            UntaggedResponse::Recent(_) => saw_recent = true,
            // RFC 9051 Section6.3.2: the solicited SELECT LIST has no NOTIFY
            // markers. Exclude marker-bearing LIST (OLDNAME,
            // \NonExistent, \NoAccess); those are NOTIFY events, not
            // the mandatory solicited response.
            UntaggedResponse::List(info) => {
                if let Some(target) = ctx.command_target()
                    && inbox_eq(target.as_str(), info.name.as_str())
                    && !is_notify_list_event(info, true)
                {
                    saw_list = true;
                }
            }
            _ => {}
        }
    }

    if !saw_flags {
        return Err(Error::Protocol(format!(
            "{command_name} completed without the required FLAGS response ({section})"
        )));
    }
    if !saw_exists {
        return Err(Error::Protocol(format!(
            "{command_name} completed without the required EXISTS response ({section})"
        )));
    }
    if is_rev2 {
        if !saw_list {
            return Err(Error::Protocol(format!(
                "{command_name} completed without the required LIST response ({section})"
            )));
        }
    } else if !saw_recent {
        return Err(Error::Protocol(format!(
            "{command_name} completed without the required RECENT response ({section})"
        )));
    }

    Ok(())
}
