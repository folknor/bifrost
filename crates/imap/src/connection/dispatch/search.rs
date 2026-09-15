use crate::connection::{NotifyFlags, SearchResult};
use crate::error::Error;
use crate::types::response::{
    EsearchResponse, ResponseCode, TaggedResponse, UntaggedResponse, UntaggedStatus,
};
use crate::types::{CopyResult, ExpungeResult, MoveResult, UidRange};

use super::super::expand_uid_ranges;
use super::{Consumer, ConsumerContext, Finalized};

/// Consumer for SEARCH and UID SEARCH (RFC 3501 Section6.4.4 / Section6.4.8).
///
/// Accumulates solicited SEARCH and ESEARCH responses. In `finalize`,
/// picks the best match with the same three-pass priority ordering
/// as the old `parse_search_result`:
/// 1. Tag-correlated ESEARCH (highest; unambiguous match)
/// 2. Tagless ESEARCH (servers that omit the correlator)
/// 3. Legacy SEARCH (`IMAP4rev1` fallback)
///
/// ESEARCH UID ranges are expanded into individual IDs. An expansion that
/// would exceed the internal safety limit fails rather than returning a
/// partial [`SearchResult`] (RFC 4731 Section3, RFC 3501 Section6.4.4).
pub(crate) struct SearchConsumer {
    /// Tag-correlated ESEARCH responses (highest priority).
    tag_correlated: Vec<EsearchResponse>,
    /// Tagless ESEARCH responses (second priority).
    tagless_esearch: Vec<EsearchResponse>,
    /// Collected legacy SEARCH responses (lowest priority).
    search_responses: Vec<(Vec<u32>, Option<u64>)>,
    /// Non-SEARCH/ESEARCH responses routed here via classification.
    buffered: Vec<UntaggedResponse>,
}

impl SearchConsumer {
    pub(crate) fn new() -> Self {
        Self {
            tag_correlated: Vec::new(),
            tagless_esearch: Vec::new(),
            search_responses: Vec::new(),
            buffered: Vec::new(),
        }
    }

    /// Everything except the response this command consumed.
    ///
    /// The chosen ESEARCH is solicited: it answers this tag, so it belongs to
    /// the command whether or not the command can turn it into a flat
    /// [`SearchResult`]. Only genuinely extra or unsolicited responses may
    /// reach the event queue, and they keep the order they arrived in.
    fn reclassified_extras(self, chosen: Chosen) -> Vec<UntaggedResponse> {
        let mut buffered = self.buffered;
        let skip_tag_correlated = usize::from(matches!(chosen, Chosen::TagCorrelated));
        for e in self.tag_correlated.into_iter().skip(skip_tag_correlated) {
            buffered.push(UntaggedResponse::Esearch(e));
        }
        let skip_tagless = usize::from(matches!(chosen, Chosen::Tagless));
        for e in self.tagless_esearch.into_iter().skip(skip_tagless) {
            buffered.push(UntaggedResponse::Esearch(e));
        }
        for (uids, mod_seq) in self.search_responses {
            buffered.push(UntaggedResponse::Search { uids, mod_seq });
        }
        buffered
    }
}

/// Which solicited response this command consumed.
#[derive(Clone, Copy)]
enum Chosen {
    TagCorrelated,
    Tagless,
}

impl Consumer for SearchConsumer {
    type Output = SearchResult;

    fn on_response(
        &mut self,
        resp: UntaggedResponse,
        _notify_snapshot: NotifyFlags,
        ctx: &ConsumerContext,
    ) {
        match resp {
            // RFC 4466 search-correlator: tag-correlated ESEARCH.
            UntaggedResponse::Esearch(e) if e.tag.as_deref() == Some(ctx.command_tag()) => {
                self.tag_correlated.push(e);
            }
            // Tagless ESEARCH. Some servers omit the correlator.
            UntaggedResponse::Esearch(e) if e.tag.is_none() => {
                self.tagless_esearch.push(e);
            }
            UntaggedResponse::Search { uids, mod_seq } => {
                self.search_responses.push((uids, mod_seq));
            }
            // Foreign-tagged ESEARCH and other response types.
            other => self.buffered.push(other),
        }
    }

    fn finalize(
        self: Box<Self>,
        tagged: TaggedResponse,
        _ctx: &ConsumerContext,
    ) -> Finalized<SearchResult> {
        if let Err(e) = tagged.require_ok() {
            // The generic `Either` buffer is surrendered; the accumulated
            // SEARCH/ESEARCH is dropped. `classify` makes both of those
            // `OnlySolicited` inside SEARCH/UID SEARCH and `Impossible`
            // anywhere else, so they are not asynchronous mailbox
            // notifications and a tagged NO cannot retroactively make them
            // into one. Re-emitting them would publish an event the
            // classifier says cannot exist. Same answer `ListConsumer` gives
            // its marker-less LIST entries for the RFC 5465 Section5.4
            // reason.
            return Finalized::failure(e, self.buffered);
        }

        // Priority: tag-correlated ESEARCH > tagless ESEARCH > legacy
        // SEARCH (RFC 4731 Section3.1, same as the old three-pass ordering).

        // Pass 1: tag-correlated ESEARCH.
        if let Some(esearch) = self.tag_correlated.first() {
            // The expansion may fail, but the response is consumed either
            // way: it is this tag's answer, so it must not be republished as
            // an asynchronous event.
            let output = expand_uid_ranges(&esearch.all).map(|ids| SearchResult {
                ids,
                mod_seq: esearch.mod_seq,
            });
            return Finalized {
                output,
                reclassified_as_events: (*self).reclassified_extras(Chosen::TagCorrelated),
            };
        }

        // Pass 2: tagless ESEARCH.
        if let Some(esearch) = self.tagless_esearch.first() {
            let output = expand_uid_ranges(&esearch.all).map(|ids| SearchResult {
                ids,
                mod_seq: esearch.mod_seq,
            });
            return Finalized {
                output,
                reclassified_as_events: (*self).reclassified_extras(Chosen::Tagless),
            };
        }

        // Pass 3: legacy SEARCH.
        let mut search_iter = self.search_responses.into_iter();
        if let Some((uids, mod_seq)) = search_iter.next() {
            let mut buffered = self.buffered;
            for (uids, mod_seq) in search_iter {
                buffered.push(UntaggedResponse::Search { uids, mod_seq });
            }
            return Finalized::success(SearchResult { ids: uids, mod_seq }, buffered);
        }

        Finalized::failure(
            Error::Protocol(
                "SEARCH OK but no untagged SEARCH/ESEARCH response \
                 (RFC 3501 Section 6.4.4)"
                    .into(),
            ),
            self.buffered,
        )
    }
}

/// Consumer for SEARCH RETURN and UID SEARCH RETURN (RFC 4731 Section3.2).
///
/// Accumulates the solicited ESEARCH response and returns the full
/// [`EsearchResponse`] with MIN, MAX, COUNT, ALL, and MODSEQ fields.
pub(crate) struct EsearchConsumer {
    /// The first matching ESEARCH response.
    result: Option<EsearchResponse>,
    /// Non-ESEARCH responses routed here via classification.
    buffered: Vec<UntaggedResponse>,
}

impl EsearchConsumer {
    pub(crate) fn new() -> Self {
        Self {
            result: None,
            buffered: Vec::new(),
        }
    }
}

impl Consumer for EsearchConsumer {
    type Output = EsearchResponse;

    fn on_response(
        &mut self,
        resp: UntaggedResponse,
        _notify_snapshot: NotifyFlags,
        ctx: &ConsumerContext,
    ) {
        match resp {
            // RFC 4731 Section3.1: server MUST return a single ESEARCH response.
            // RFC 4466 search-correlator: accept tag-correlated or tagless
            // ESEARCH. Foreign-tagged ESEARCH belongs to another context.
            // Take the first matching one; extras are reclassified.
            UntaggedResponse::Esearch(e)
                if self.result.is_none()
                    && (e.tag.is_none() || e.tag.as_deref() == Some(ctx.command_tag())) =>
            {
                self.result = Some(e);
            }
            other => self.buffered.push(other),
        }
    }

    fn finalize(
        self: Box<Self>,
        tagged: TaggedResponse,
        _ctx: &ConsumerContext,
    ) -> Finalized<EsearchResponse> {
        let buffered = self.buffered;

        if let Err(e) = tagged.require_ok() {
            // Surrender the generic `Either` buffer, drop the accumulated
            // ESEARCH: it is `OnlySolicited` here and `Impossible` outside a
            // search command, so it has no life as an asynchronous event.
            return Finalized::failure(e, buffered);
        }

        let output = self.result.ok_or_else(|| {
            Error::Protocol(
                "SEARCH RETURN OK but no ESEARCH response \
                 (RFC 4731 Section 3.1)"
                    .into(),
            )
        });

        Finalized {
            output,
            reclassified_as_events: buffered,
        }
    }
}

/// Consumer for SEARCH RETURN (SAVE) and UID SEARCH RETURN (SAVE)
/// (RFC 5182 Section2).
///
/// The server saves results server-side. RFC 5182 Section2 requires a
/// solicited SEARCH or ESEARCH echo, but some servers (e.g. Dovecot)
/// omit it. Per Postel's law we tolerate the omission. The consumer
/// discards any SEARCH/ESEARCH data and succeeds on tagged OK.
pub(crate) struct SearchSaveConsumer {
    /// Non-SEARCH/ESEARCH responses routed here via classification.
    buffered: Vec<UntaggedResponse>,
}

impl SearchSaveConsumer {
    pub(crate) fn new() -> Self {
        Self {
            buffered: Vec::new(),
        }
    }
}

impl Consumer for SearchSaveConsumer {
    type Output = ();

    fn on_response(
        &mut self,
        resp: UntaggedResponse,
        _notify_snapshot: NotifyFlags,
        ctx: &ConsumerContext,
    ) {
        match resp {
            // RFC 5182 Section2: the server MUST send a solicited SEARCH or
            // ESEARCH even for SAVE-only requests. We accept and discard
            // the data; the caller only needs tagged OK.
            UntaggedResponse::Search { .. } => {}
            // RFC 4466 search-correlator: only accept tag-correlated or
            // tagless ESEARCH. Foreign-tagged ESEARCH is not solicited.
            UntaggedResponse::Esearch(e)
                if e.tag.is_none() || e.tag.as_deref() == Some(ctx.command_tag()) => {}
            other => self.buffered.push(other),
        }
    }

    fn finalize(self: Box<Self>, tagged: TaggedResponse, _ctx: &ConsumerContext) -> Finalized<()> {
        // The SEARCH/ESEARCH data was already discarded at accumulation time
        // (RFC 5182 Section2 SAVE-only), so only the generic `Either` buffer
        // is left, and it is surrendered on both arms.
        Finalized {
            output: tagged.require_ok().map(|_| ()),
            reclassified_as_events: self.buffered,
        }
    }
}

/// Consumer for COPY and UID COPY (RFC 3501 Section6.4.7, RFC 4315 Section3).
///
/// COPY has no solicited untagged responses. The result is extracted
/// from the tagged OK response code, which SHOULD be `[COPYUID ...]`
/// per RFC 4315 Section3. Any untagged responses routed here (classified as
/// `Either`) are reclassified as events.
pub(crate) struct CopyConsumer {
    /// All responses routed here. COPY has no solicited untagged
    /// responses, so everything is reclassified as events.
    buffered: Vec<UntaggedResponse>,
    /// COPYUID response code extracted from an untagged `* OK [COPYUID ...]`.
    /// Some servers (e.g. Dovecot) send COPYUID in an untagged OK rather
    /// than in the tagged OK (RFC 4315 Section3).
    code: Option<ResponseCode>,
}

impl CopyConsumer {
    pub(crate) fn new() -> Self {
        Self {
            buffered: Vec::new(),
            code: None,
        }
    }
}

impl Consumer for CopyConsumer {
    type Output = CopyResult;

    fn on_response(
        &mut self,
        resp: UntaggedResponse,
        _notify_snapshot: NotifyFlags,
        _ctx: &ConsumerContext,
    ) {
        // COPY has no solicited untagged responses (RFC 3501 Section6.4.7).
        // Buffer everything for reclassification as events.
        match resp {
            // RFC 4315 Section3: some servers send COPYUID in an untagged OK.
            UntaggedResponse::Status {
                status: UntaggedStatus::Ok,
                code: code_opt @ Some(ResponseCode::CopyUid { .. }),
                ..
            } if self.code.is_none() => {
                self.code = code_opt;
            }
            other => self.buffered.push(other),
        }
    }

    fn finalize(
        self: Box<Self>,
        tagged: TaggedResponse,
        _ctx: &ConsumerContext,
    ) -> Finalized<CopyResult> {
        // RFC 4315 Section3: server SHOULD return COPYUID response code.
        match tagged.require_ok() {
            Ok(tagged) => Finalized::success(
                CopyResult {
                    code: tagged.code.or(self.code),
                },
                self.buffered,
            ),
            // The `Either` buffer is surrendered: COPY solicits no untagged
            // response, so everything in it is an asynchronous notification
            // and survives the failure. The captured untagged
            // `OK [COPYUID ...]` is not re-emitted: it is this command's own
            // answer, and only its response code was retained anyway, so
            // there is nothing faithful left to publish.
            Err(e) => Finalized::failure(e, self.buffered),
        }
    }
}

/// Consumer for MOVE and UID MOVE (RFC 6851 Section3).
///
/// Accumulates EXPUNGE (RFC 3501 Section7.4.1) and VANISHED (RFC 7162
/// Section3.2.10) responses sent by the server before the tagged OK.
/// Returns a [`MoveResult`] with the COPYUID response code and the
/// expunged sequence numbers or UID ranges.
///
/// When QRESYNC is enabled the server sends VANISHED instead of
/// EXPUNGE (RFC 7162 Section3.2.10). The consumer accumulates both
/// variants and selects the appropriate [`ExpungeResult`] variant
/// based on the QRESYNC enabled state in `finalize`.
pub(crate) struct MoveConsumer {
    /// `* N EXPUNGE` and `* VANISHED ...` responses, kept verbatim and in
    /// arrival order.
    ///
    /// These are stored as whole responses rather than as pre-flattened
    /// `Vec<u32>` / `Vec<UidRange>` accumulators because the failure arm has
    /// to re-emit them: MOVE CLAIMS them into its typed [`MoveResult`], so
    /// nothing upstream can preserve them on its behalf. Flattening at
    /// accumulation time would have destroyed the `earlier` flag, the
    /// per-response boundaries, and the arrival order between EXPUNGE and
    /// VANISHED - and a reconstruction that guesses any of those
    /// misrepresents what the server sent.
    mutations: Vec<UntaggedResponse>,
    /// Non-EXPUNGE/VANISHED responses for reclassification.
    buffered: Vec<UntaggedResponse>,
    /// COPYUID response code extracted from an untagged `* OK [COPYUID ...]`.
    /// Some servers (e.g. Dovecot) send COPYUID in an untagged OK rather
    /// than in the tagged OK (RFC 4315 Section3).
    code: Option<ResponseCode>,
}

impl MoveConsumer {
    pub(crate) fn new() -> Self {
        Self {
            mutations: Vec::new(),
            buffered: Vec::new(),
            code: None,
        }
    }
}

impl Consumer for MoveConsumer {
    type Output = MoveResult;

    fn on_response(
        &mut self,
        resp: UntaggedResponse,
        _notify_snapshot: NotifyFlags,
        _ctx: &ConsumerContext,
    ) {
        match resp {
            // RFC 6851 Section3: EXPUNGE responses for moved messages.
            // RFC 7162 Section3.2.10: VANISHED instead, when QRESYNC is on.
            resp @ (UntaggedResponse::Expunge(_) | UntaggedResponse::Vanished { .. }) => {
                self.mutations.push(resp);
            }
            // RFC 4315 Section3: some servers send COPYUID in an untagged OK.
            UntaggedResponse::Status {
                status: UntaggedStatus::Ok,
                code: code_opt @ Some(ResponseCode::CopyUid { .. }),
                ..
            } if self.code.is_none() => {
                self.code = code_opt;
            }
            other => self.buffered.push(other),
        }
    }

    fn finalize(
        self: Box<Self>,
        tagged: TaggedResponse,
        ctx: &ConsumerContext,
    ) -> Finalized<MoveResult> {
        match tagged.require_ok() {
            Ok(tagged) => Finalized::success(
                MoveResult {
                    // RFC 6851 Section4.3: MOVE SHOULD return COPYUID.
                    code: tagged.code.or(self.code),
                    expunged: fold_mutations(self.mutations, qresync_enabled(ctx)),
                },
                self.buffered,
            ),
            Err(e) => {
                // The sharpest failure arm in the crate. A tagged NO can
                // follow `* 3 EXPUNGE`: the server really did expunge, and
                // MOVE claims those responses into its typed result, so if
                // this arm drops them the evidence of a performed mutation is
                // gone for good and a per-folder modseq cache entry is never
                // cleared. They are `Either`-classified here anyway
                // (`(_, Expunge)` and `(_, Vanished { earlier: false })`), so
                // the generic rule reaches them: surrender both the buffer and
                // the mutations, in that order. They are re-emitted verbatim,
                // not reconstructed, which is why they were never flattened.
                let mut reclassified = self.buffered;
                reclassified.extend(self.mutations);
                Finalized::failure(e, reclassified)
            }
        }
    }
}

/// Consumer for EXPUNGE (RFC 3501 Section6.4.3) and UID EXPUNGE (RFC 4315 Section2).
///
/// Accumulates EXPUNGE sequence numbers and VANISHED UID ranges.
/// When QRESYNC is enabled the server sends VANISHED instead of
/// EXPUNGE (RFC 7162 Section3.2.10).
pub(crate) struct ExpungeConsumer {
    /// `* N EXPUNGE` and `* VANISHED [(EARLIER)] ...` responses, verbatim and
    /// in arrival order. See `MoveConsumer::mutations` for why these are not
    /// flattened at accumulation time; here the `earlier` flag matters even
    /// more, because `classify` admits BOTH `VANISHED` and `VANISHED
    /// (EARLIER)` under `CK::Expunge` and the two mean different things to a
    /// downstream reader.
    mutations: Vec<UntaggedResponse>,
    /// Non-EXPUNGE/VANISHED responses for reclassification.
    buffered: Vec<UntaggedResponse>,
}

impl ExpungeConsumer {
    pub(crate) fn new() -> Self {
        Self {
            mutations: Vec::new(),
            buffered: Vec::new(),
        }
    }
}

/// Whether QRESYNC was successfully `ENABLE`d (RFC 7162 Section3.2.10:
/// the server then sends VANISHED instead of EXPUNGE).
///
/// The ENABLED echo is stored verbatim; the name is an atom (RFC 9051
/// Section 9), so compare case-insensitively.
fn qresync_enabled(ctx: &ConsumerContext) -> bool {
    ctx.enabled()
        .iter()
        .any(|e| e.eq_ignore_ascii_case("QRESYNC"))
}

/// Flatten accumulated EXPUNGE/VANISHED responses into the typed result.
///
/// Selects the variant by QRESYNC enabled state rather than by what actually
/// arrived, matching the RFC 7162 Section3.2.10 contract.
fn fold_mutations(mutations: Vec<UntaggedResponse>, qresync: bool) -> ExpungeResult {
    if qresync {
        let mut vanished: Vec<UidRange> = Vec::new();
        for resp in mutations {
            if let UntaggedResponse::Vanished { uids, .. } = resp {
                vanished.extend(uids);
            }
        }
        ExpungeResult::Vanished(vanished)
    } else {
        let mut expunged = Vec::new();
        for resp in mutations {
            if let UntaggedResponse::Expunge(n) = resp {
                expunged.push(n);
            }
        }
        ExpungeResult::Expunged(expunged)
    }
}

impl Consumer for ExpungeConsumer {
    type Output = ExpungeResult;

    fn on_response(
        &mut self,
        resp: UntaggedResponse,
        _notify_snapshot: NotifyFlags,
        _ctx: &ConsumerContext,
    ) {
        match resp {
            // RFC 3501 Section7.4.1: EXPUNGE with sequence numbers.
            // RFC 7162 Section3.2.10: VANISHED with UID ranges when QRESYNC
            // is enabled.
            resp @ (UntaggedResponse::Expunge(_) | UntaggedResponse::Vanished { .. }) => {
                self.mutations.push(resp);
            }
            other => self.buffered.push(other),
        }
    }

    fn finalize(
        self: Box<Self>,
        tagged: TaggedResponse,
        ctx: &ConsumerContext,
    ) -> Finalized<ExpungeResult> {
        match tagged.require_ok() {
            Ok(_) => Finalized::success(
                fold_mutations(self.mutations, qresync_enabled(ctx)),
                self.buffered,
            ),
            Err(e) => {
                // A failed EXPUNGE may still have expunged something: the
                // server can report `* 1 EXPUNGE` and then fail the command
                // partway. These accumulators are `OnlySolicited` under
                // `CK::Expunge`, so the `Either` rule does not reach them and
                // this is a decision on its own merits - but an EXPUNGE that
                // the client never hears about leaves the message cache and
                // the per-folder modseq permanently ahead of the mailbox,
                // which is worse than publishing an event for a mutation the
                // server did perform. Surrendered verbatim, so the `earlier`
                // flag and the arrival order are intact.
                let mut reclassified = self.buffered;
                reclassified.extend(self.mutations);
                Finalized::failure(e, reclassified)
            }
        }
    }
}
