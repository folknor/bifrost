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
/// ESEARCH UID ranges are expanded into individual IDs. The `truncated`
/// flag on [`SearchResult`] signals when the expansion was capped at the
/// internal safety limit (RFC 4731 Section3, RFC 3501 Section6.4.4).
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

    /// Drain all accumulated responses into `buffered` for reclassification.
    fn drain_all_into_buffered(&mut self) {
        for e in self.tag_correlated.drain(..) {
            self.buffered.push(UntaggedResponse::Esearch(e));
        }
        for e in self.tagless_esearch.drain(..) {
            self.buffered.push(UntaggedResponse::Esearch(e));
        }
        for (uids, mod_seq) in self.search_responses.drain(..) {
            self.buffered
                .push(UntaggedResponse::Search { uids, mod_seq });
        }
    }
}

impl Consumer for SearchConsumer {
    /// `Result` wrapper ensures `reclassified_as_events` is always
    /// processed even when the command-level outcome is an error
    /// (e.g., no solicited response found). Callers flatten with `??`.
    type Output = Result<SearchResult, Error>;

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
        mut self: Box<Self>,
        tagged: TaggedResponse,
        _ctx: &ConsumerContext,
    ) -> Result<Finalized<Result<SearchResult, Error>>, Error> {
        if let Err(e) = tagged.require_ok() {
            self.drain_all_into_buffered();
            return Ok(Finalized {
                output: Err(e),
                reclassified_as_events: self.buffered,
            });
        }

        // Priority: tag-correlated ESEARCH > tagless ESEARCH > legacy
        // SEARCH (RFC 4731 Section3.1, same as the old three-pass ordering).

        // Pass 1: tag-correlated ESEARCH.
        if let Some(esearch) = self.tag_correlated.first() {
            let (ids, truncated) = expand_uid_ranges(&esearch.all);
            let result = SearchResult {
                ids,
                mod_seq: esearch.mod_seq,
                truncated,
            };
            // Consumed the first tag-correlated; reclassify the rest.
            let mut buffered = self.buffered;
            for e in self.tag_correlated.into_iter().skip(1) {
                buffered.push(UntaggedResponse::Esearch(e));
            }
            for e in self.tagless_esearch {
                buffered.push(UntaggedResponse::Esearch(e));
            }
            for (uids, mod_seq) in self.search_responses {
                buffered.push(UntaggedResponse::Search { uids, mod_seq });
            }
            return Ok(Finalized {
                output: Ok(result),
                reclassified_as_events: buffered,
            });
        }

        // Pass 2: tagless ESEARCH.
        if let Some(esearch) = self.tagless_esearch.first() {
            let (ids, truncated) = expand_uid_ranges(&esearch.all);
            let result = SearchResult {
                ids,
                mod_seq: esearch.mod_seq,
                truncated,
            };
            let mut buffered = self.buffered;
            for e in self.tagless_esearch.into_iter().skip(1) {
                buffered.push(UntaggedResponse::Esearch(e));
            }
            for (uids, mod_seq) in self.search_responses {
                buffered.push(UntaggedResponse::Search { uids, mod_seq });
            }
            return Ok(Finalized {
                output: Ok(result),
                reclassified_as_events: buffered,
            });
        }

        // Pass 3: legacy SEARCH.
        let mut search_iter = self.search_responses.into_iter();
        if let Some((uids, mod_seq)) = search_iter.next() {
            let mut buffered = self.buffered;
            for (uids, mod_seq) in search_iter {
                buffered.push(UntaggedResponse::Search { uids, mod_seq });
            }
            return Ok(Finalized {
                output: Ok(SearchResult {
                    ids: uids,
                    mod_seq,
                    truncated: false,
                }),
                reclassified_as_events: buffered,
            });
        }

        Ok(Finalized {
            output: Err(Error::Protocol(
                "SEARCH OK but no untagged SEARCH/ESEARCH response \
                 (RFC 3501 Section 6.4.4)"
                    .into(),
            )),
            reclassified_as_events: self.buffered,
        })
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
    /// `Result` wrapper ensures `reclassified_as_events` is always
    /// processed even when the command-level outcome is an error.
    type Output = Result<EsearchResponse, Error>;

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
    ) -> Result<Finalized<Result<EsearchResponse, Error>>, Error> {
        let buffered = self.buffered;

        if let Err(e) = tagged.require_ok() {
            return Ok(Finalized {
                output: Err(e),
                reclassified_as_events: buffered,
            });
        }

        let output = self.result.ok_or_else(|| {
            Error::Protocol(
                "SEARCH RETURN OK but no ESEARCH response \
                 (RFC 4731 Section 3.1)"
                    .into(),
            )
        });

        Ok(Finalized {
            output,
            reclassified_as_events: buffered,
        })
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
    /// `Result` wrapper ensures `reclassified_as_events` is always
    /// processed even when the command-level outcome is an error.
    type Output = Result<(), Error>;

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

    fn finalize(
        self: Box<Self>,
        tagged: TaggedResponse,
        _ctx: &ConsumerContext,
    ) -> Result<Finalized<Result<(), Error>>, Error> {
        Ok(Finalized {
            output: tagged.require_ok().map(|_| ()),
            reclassified_as_events: self.buffered,
        })
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
    ) -> Result<Finalized<CopyResult>, Error> {
        // RFC 4315 Section3: server SHOULD return COPYUID response code.
        let tagged = tagged.require_ok()?;
        Ok(Finalized {
            output: CopyResult {
                code: tagged.code.or(self.code),
            },
            reclassified_as_events: self.buffered,
        })
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
    /// Expunged sequence numbers from `* N EXPUNGE` responses.
    expunged: Vec<u32>,
    /// Vanished UID ranges from `* VANISHED ...` responses.
    vanished: Vec<UidRange>,
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
            expunged: Vec::new(),
            vanished: Vec::new(),
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
            UntaggedResponse::Expunge(n) => {
                self.expunged.push(n);
            }
            // RFC 7162 Section3.2.10: VANISHED responses when QRESYNC is enabled.
            UntaggedResponse::Vanished { uids, .. } => {
                self.vanished.extend(uids);
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
    ) -> Result<Finalized<MoveResult>, Error> {
        let tagged = tagged.require_ok()?;
        // RFC 7162 Section3.2.10: when QRESYNC is enabled, the server sends
        // VANISHED instead of EXPUNGE.
        let expunged = if ctx.enabled().iter().any(|e| e == "QRESYNC") {
            ExpungeResult::Vanished(self.vanished)
        } else {
            ExpungeResult::Expunged(self.expunged)
        };
        Ok(Finalized {
            output: MoveResult {
                // RFC 6851 Section4.3: MOVE SHOULD return COPYUID response code.
                code: tagged.code.or(self.code),
                expunged,
            },
            reclassified_as_events: self.buffered,
        })
    }
}

/// Consumer for EXPUNGE (RFC 3501 Section6.4.3) and UID EXPUNGE (RFC 4315 Section2).
///
/// Accumulates EXPUNGE sequence numbers and VANISHED UID ranges.
/// When QRESYNC is enabled the server sends VANISHED instead of
/// EXPUNGE (RFC 7162 Section3.2.10).
pub(crate) struct ExpungeConsumer {
    /// Expunged sequence numbers from `* N EXPUNGE` responses.
    expunged: Vec<u32>,
    /// Vanished UID ranges from `* VANISHED (EARLIER) ...` responses.
    vanished: Vec<UidRange>,
    /// Non-EXPUNGE/VANISHED responses for reclassification.
    buffered: Vec<UntaggedResponse>,
}

impl ExpungeConsumer {
    pub(crate) fn new() -> Self {
        Self {
            expunged: Vec::new(),
            vanished: Vec::new(),
            buffered: Vec::new(),
        }
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
            // RFC 3501 Section7.4.1: EXPUNGE responses with sequence numbers.
            UntaggedResponse::Expunge(n) => {
                self.expunged.push(n);
            }
            // RFC 7162 Section3.2.10: VANISHED (EARLIER) with UID ranges when
            // QRESYNC is enabled.
            UntaggedResponse::Vanished { uids, .. } => {
                self.vanished.extend(uids);
            }
            other => self.buffered.push(other),
        }
    }

    fn finalize(
        self: Box<Self>,
        tagged: TaggedResponse,
        ctx: &ConsumerContext,
    ) -> Result<Finalized<ExpungeResult>, Error> {
        tagged.require_ok()?;
        // RFC 7162 Section3.2.10: when QRESYNC is enabled, the server sends
        // VANISHED instead of EXPUNGE.
        let result = if ctx.enabled().iter().any(|e| e == "QRESYNC") {
            ExpungeResult::Vanished(self.vanished)
        } else {
            ExpungeResult::Expunged(self.expunged)
        };
        Ok(Finalized {
            output: result,
            reclassified_as_events: self.buffered,
        })
    }
}
