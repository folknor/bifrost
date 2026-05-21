use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;

use crate::connection::NotifyFlags;
use crate::error::Error;
use crate::types::response::{TaggedResponse, UntaggedResponse};
use crate::types::validated::ParsedUidSet;
use crate::types::{FetchResponse, StoreResult, UidRange};

use super::{BackpressureState, Consumer, ConsumerContext, Finalized, StreamingConsumer};

/// Default warn-on-large threshold in bytes (10 MB).
///
/// When the estimated accumulated size of buffered `FetchResponse`s
/// exceeds this limit, a `tracing::warn!` is emitted pointing the
/// caller towards `uid_fetch_streaming`.
pub(crate) const DEFAULT_FETCH_WARN_BYTES: usize = 10 * 1024 * 1024;

/// Rough byte-size estimate for a single [`FetchResponse`].
///
/// Sums the data lengths of body sections and binary sections (the
/// dominant contributors to memory), plus a flat overhead per response
/// for the fixed fields and heap-allocated strings.
pub(crate) fn estimate_fetch_response_bytes(fr: &FetchResponse) -> usize {
    // Flat overhead: seq/uid/flags/envelope/bodystructure/dates/ids etc.
    // Conservative estimate: covers the struct itself plus typical
    // small-string heap allocations.
    let mut size: usize = 256;
    for bs in &fr.body_sections {
        size += bs.data.as_ref().map_or(0, Vec::len);
    }
    for bin in &fr.binary_sections {
        size += bin.data.as_ref().map_or(0, Vec::len);
    }
    size
}

/// Consumer for FETCH / UID FETCH (RFC 3501 Section6.4.5, buffering form).
///
/// Accumulates `FETCH` untagged responses into a `Vec<FetchResponse>`.
/// Logs a warning when the accumulated byte estimate exceeds a
/// configurable threshold (default 10 MB) to nudge callers toward the
/// streaming variant (`uid_fetch_streaming`).
pub(crate) struct FetchConsumer {
    fetches: Vec<FetchResponse>,
    /// Non-FETCH responses routed here (classified as `Either`).
    buffered: Vec<UntaggedResponse>,
    /// Running byte-size estimate of accumulated FETCH data.
    estimated_bytes: usize,
    /// Threshold at which to emit a warn-on-large log.
    warn_threshold: usize,
    /// Hard caller-supplied memory budget.
    hard_limit: Option<usize>,
    /// Fetch-limit error captured when the hard limit was first crossed.
    limit_exceeded: Option<Error>,
    /// Whether the warning has already been emitted (log once).
    warned: bool,
}

impl FetchConsumer {
    pub(crate) fn new() -> Self {
        Self {
            fetches: Vec::new(),
            buffered: Vec::new(),
            estimated_bytes: 0,
            warn_threshold: DEFAULT_FETCH_WARN_BYTES,
            hard_limit: None,
            limit_exceeded: None,
            warned: false,
        }
    }

    pub(crate) fn with_limit(limit: usize) -> Self {
        Self {
            hard_limit: Some(limit),
            ..Self::new()
        }
    }
}

impl Consumer for FetchConsumer {
    type Output = Vec<FetchResponse>;

    fn on_response(
        &mut self,
        resp: UntaggedResponse,
        _notify_snapshot: NotifyFlags,
        _ctx: &ConsumerContext,
    ) {
        // RFC 3501 Section7.4.2: FETCH responses are the solicited data
        // for FETCH/UID FETCH commands.
        if let UntaggedResponse::Fetch(fr) = resp {
            self.estimated_bytes = self
                .estimated_bytes
                .saturating_add(estimate_fetch_response_bytes(&fr));
            if let Some(limit) = self.hard_limit
                && self.estimated_bytes > limit
            {
                self.limit_exceeded.get_or_insert(Error::FetchLimit {
                    estimated: self.estimated_bytes,
                    limit,
                    seq: fr.seq,
                    uid: fr.uid,
                });
                return;
            }
            if !self.warned && self.estimated_bytes > self.warn_threshold {
                tracing::warn!(
                    estimated_bytes = self.estimated_bytes,
                    threshold = self.warn_threshold,
                    "FETCH response buffer exceeds {} MB; consider \
                     uid_fetch_streaming for large result sets",
                    self.warn_threshold / (1024 * 1024),
                );
                self.warned = true;
            }
            self.fetches.push(*fr);
        } else {
            // Non-FETCH responses (EXISTS, EXPUNGE, FLAGS, etc.)
            // classified as Either; reclassify as events.
            self.buffered.push(resp);
        }
    }

    fn finalize(
        self: Box<Self>,
        tagged: TaggedResponse,
        _ctx: &ConsumerContext,
    ) -> Result<Finalized<Vec<FetchResponse>>, Error> {
        tagged.require_ok()?;
        if let Some(error) = self.limit_exceeded {
            return Err(error);
        }
        Ok(Finalized {
            output: self.fetches,
            reclassified_as_events: self.buffered,
        })
    }
}

/// Streaming consumer for FETCH / UID FETCH (RFC 3501 Section6.4.5).
///
/// Instead of buffering all `FETCH` responses into a `Vec`, pushes each
/// one through an `mpsc::UnboundedSender` as it arrives. The dispatcher keeps
/// reading until the tagged OK regardless of whether the receiver is
/// still alive; this keeps the IMAP stream consistent.
///
/// Non-FETCH responses classified as `Either` are buffered and returned
/// in `finalize` for the dispatcher to re-emit as events.
#[cfg(test)]
pub(crate) struct StreamingFetchConsumer {
    tx: tokio::sync::mpsc::UnboundedSender<Result<FetchResponse, Error>>,
    /// Buffer for ambiguous responses the dispatcher routed here but
    /// that finalize will re-emit as events.
    ambiguous_buffer: Vec<UntaggedResponse>,
}

#[cfg(test)]
impl StreamingFetchConsumer {
    pub(crate) fn new(
        tx: tokio::sync::mpsc::UnboundedSender<Result<FetchResponse, Error>>,
    ) -> Self {
        Self {
            tx,
            ambiguous_buffer: Vec::new(),
        }
    }
}

struct BoundedStreamingPipe<T> {
    tx: Option<tokio::sync::mpsc::Sender<Result<T, Error>>>,
    permit: Option<tokio::sync::mpsc::OwnedPermit<Result<T, Error>>>,
    pending: VecDeque<T>,
    drained: bool,
}

impl<T: Send + 'static> BoundedStreamingPipe<T> {
    fn new(tx: tokio::sync::mpsc::Sender<Result<T, Error>>) -> Self {
        Self {
            tx: Some(tx),
            permit: None,
            pending: VecDeque::new(),
            drained: false,
        }
    }

    fn push(&mut self, item: T) {
        if self.drained {
            return;
        }
        if let Some(permit) = self.permit.take() {
            permit.send(Ok(item));
        } else {
            self.pending.push_back(item);
        }
    }

    fn backpressure_state(&self) -> BackpressureState {
        if self.drained {
            BackpressureState::Drained
        } else if self.permit.is_some() {
            BackpressureState::Ready
        } else {
            BackpressureState::NeedsCapacity
        }
    }

    fn reserve_capacity(&mut self) -> Pin<Box<dyn Future<Output = Result<(), Error>> + Send + '_>> {
        Box::pin(async move {
            if self.drained || self.permit.is_some() {
                return Ok(());
            }

            let Some(tx) = self.tx.as_ref().cloned() else {
                self.drained = true;
                self.pending.clear();
                return Ok(());
            };

            match tx.reserve_owned().await {
                Ok(permit) => {
                    if let Some(item) = self.pending.pop_front() {
                        permit.send(Ok(item));
                    } else {
                        self.permit = Some(permit);
                    }
                    Ok(())
                }
                Err(_) => {
                    self.drained = true;
                    self.tx = None;
                    self.permit = None;
                    self.pending.clear();
                    Ok(())
                }
            }
        })
    }
}

/// Bounded streaming consumer for FETCH / UID FETCH.
///
/// Capacity is pre-reserved by the driver before each read. If the
/// receiver stops polling, the driver stops reading and TCP backpressure
/// reaches the server.
pub(crate) struct BoundedStreamingFetchConsumer {
    pipe: BoundedStreamingPipe<FetchResponse>,
    ambiguous_buffer: Vec<UntaggedResponse>,
}

impl BoundedStreamingFetchConsumer {
    pub(crate) fn new(tx: tokio::sync::mpsc::Sender<Result<FetchResponse, Error>>) -> Self {
        Self {
            pipe: BoundedStreamingPipe::new(tx),
            ambiguous_buffer: Vec::new(),
        }
    }
}

impl Consumer for BoundedStreamingFetchConsumer {
    type Output = ();

    fn on_response(
        &mut self,
        resp: UntaggedResponse,
        _notify_snapshot: NotifyFlags,
        _ctx: &ConsumerContext,
    ) {
        if let UntaggedResponse::Fetch(fr) = resp {
            self.pipe.push(*fr);
        } else {
            self.ambiguous_buffer.push(resp);
        }
    }

    fn finalize(
        self: Box<Self>,
        tagged: TaggedResponse,
        _ctx: &ConsumerContext,
    ) -> Result<Finalized<()>, Error> {
        tagged.require_ok()?;
        Ok(Finalized {
            output: (),
            reclassified_as_events: self.ambiguous_buffer,
        })
    }
}

impl StreamingConsumer for BoundedStreamingFetchConsumer {
    fn backpressure_state(&self) -> BackpressureState {
        self.pipe.backpressure_state()
    }

    fn reserve_capacity(&mut self) -> Pin<Box<dyn Future<Output = Result<(), Error>> + Send + '_>> {
        self.pipe.reserve_capacity()
    }
}

#[cfg(test)]
impl Consumer for StreamingFetchConsumer {
    type Output = ();

    fn on_response(
        &mut self,
        resp: UntaggedResponse,
        _notify_snapshot: NotifyFlags,
        _ctx: &ConsumerContext,
    ) {
        // RFC 3501 Section7.4.2: FETCH responses are the solicited data
        // for FETCH/UID FETCH commands.
        if let UntaggedResponse::Fetch(fr) = resp {
            // If the receiver is dropped, discard the response but keep
            // reading until the tagged OK to preserve stream consistency.
            let _ = self.tx.send(Ok(*fr));
        } else {
            // Non-FETCH response routed to us; ambiguous.
            self.ambiguous_buffer.push(resp);
        }
    }

    fn finalize(
        self: Box<Self>,
        tagged: TaggedResponse,
        _ctx: &ConsumerContext,
    ) -> Result<Finalized<()>, Error> {
        tagged.require_ok()?;
        // Drop self.tx by consuming self; this signals end of stream.
        Ok(Finalized {
            output: (),
            reclassified_as_events: self.ambiguous_buffer,
        })
    }
}

/// Item yielded by the VANISHED-aware streaming FETCH consumer.
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::large_enum_variant)]
#[allow(dead_code)]
pub(crate) enum FetchStreamItem {
    Fetch(FetchResponse),
    VanishedEarlier(Vec<UidRange>),
}

/// Bounded streaming consumer for UID FETCH CHANGEDSINCE VANISHED.
#[allow(dead_code)]
pub(crate) struct BoundedStreamingFetchVanishedConsumer {
    pipe: BoundedStreamingPipe<FetchStreamItem>,
    requested_set: Option<ParsedUidSet>,
    dropped_vanished_count: usize,
    buffered: Vec<UntaggedResponse>,
}

#[allow(dead_code)]
impl BoundedStreamingFetchVanishedConsumer {
    pub(crate) fn new(
        tx: tokio::sync::mpsc::Sender<Result<FetchStreamItem, Error>>,
        requested_set: Option<ParsedUidSet>,
    ) -> Self {
        Self {
            pipe: BoundedStreamingPipe::new(tx),
            requested_set,
            dropped_vanished_count: 0,
            buffered: Vec::new(),
        }
    }
}

impl Consumer for BoundedStreamingFetchVanishedConsumer {
    type Output = ();

    fn on_response(
        &mut self,
        resp: UntaggedResponse,
        _notify_snapshot: NotifyFlags,
        _ctx: &ConsumerContext,
    ) {
        match resp {
            UntaggedResponse::Fetch(fr) => self.pipe.push(FetchStreamItem::Fetch(*fr)),
            UntaggedResponse::Vanished {
                earlier: true,
                uids,
            } => {
                let filtered = if let Some(ref set) = self.requested_set {
                    let (filtered, dropped) = set.intersect_uid_ranges(&uids);
                    self.dropped_vanished_count += dropped;
                    filtered
                } else {
                    uids
                };
                if !filtered.is_empty() {
                    self.pipe.push(FetchStreamItem::VanishedEarlier(filtered));
                }
            }
            other => self.buffered.push(other),
        }
    }

    fn finalize(
        self: Box<Self>,
        tagged: TaggedResponse,
        _ctx: &ConsumerContext,
    ) -> Result<Finalized<()>, Error> {
        tagged.require_ok()?;
        if self.dropped_vanished_count > 0 {
            tracing::debug!(
                dropped = self.dropped_vanished_count,
                "filtered out-of-set VANISHED (EARLIER) UIDs per RFC 7162 Section 3.2.6",
            );
        }
        Ok(Finalized {
            output: (),
            reclassified_as_events: self.buffered,
        })
    }
}

impl StreamingConsumer for BoundedStreamingFetchVanishedConsumer {
    fn backpressure_state(&self) -> BackpressureState {
        self.pipe.backpressure_state()
    }

    fn reserve_capacity(&mut self) -> Pin<Box<dyn Future<Output = Result<(), Error>> + Send + '_>> {
        self.pipe.reserve_capacity()
    }
}

/// Consumer for STORE / UID STORE (RFC 3501 Section6.4.6, RFC 7162 Section3.1.3).
///
/// Accumulates the implicit FETCH responses that non-`.SILENT` STORE
/// operations produce (RFC 3501 Section6.4.6: "the server SHOULD send an
/// untagged FETCH response for each message whose flags were updated").
/// Also extracts the tagged OK response code, which may contain
/// `[MODIFIED ...]` when UNCHANGEDSINCE was used (RFC 7162 Section3.1.3).
pub(crate) struct StoreConsumer {
    fetches: Vec<FetchResponse>,
    /// Non-FETCH responses routed here (classified as `Either`).
    buffered: Vec<UntaggedResponse>,
}

impl StoreConsumer {
    pub(crate) fn new() -> Self {
        Self {
            fetches: Vec::new(),
            buffered: Vec::new(),
        }
    }
}

impl Consumer for StoreConsumer {
    type Output = StoreResult;

    fn on_response(
        &mut self,
        resp: UntaggedResponse,
        _notify_snapshot: NotifyFlags,
        _ctx: &ConsumerContext,
    ) {
        // RFC 3501 Section6.4.6: STORE returns implicit FETCH responses
        // with updated flags for each message whose flags were changed.
        // `.SILENT` operations suppress these.
        if let UntaggedResponse::Fetch(fr) = resp {
            self.fetches.push(*fr);
        } else {
            self.buffered.push(resp);
        }
    }

    fn finalize(
        self: Box<Self>,
        tagged: TaggedResponse,
        _ctx: &ConsumerContext,
    ) -> Result<Finalized<StoreResult>, Error> {
        let tagged = tagged.require_ok()?;
        // RFC 7162 Section3.1.3: preserve [MODIFIED sequence-set] from
        // tagged OK when UNCHANGEDSINCE was used.
        Ok(Finalized {
            output: StoreResult {
                fetches: self.fetches,
                code: tagged.code,
            },
            reclassified_as_events: self.buffered,
        })
    }
}

/// Consumer for UID FETCH with VANISHED modifier
/// (RFC 7162 Section3.2.6).
///
/// Accumulates both `FETCH` responses and `VANISHED (EARLIER)`
/// responses. Plain `VANISHED` (earlier: false) are unsolicited
/// real-time expunge notifications and are reclassified as events.
///
/// When `requested_set` is `Some`, `VANISHED (EARLIER)` UIDs are
/// defensively filtered to only include UIDs within the requested
/// set. RFC 7162 Section 3.2.6 says the server SHOULD limit these
/// responses, but non-conformant servers (e.g. Stalwart) may return
/// UIDs outside the requested set. When `requested_set` is `None`
/// (because the sequence set contained `$`, an unresolvable search
/// result reference per RFC 5182), filtering is skipped.
pub(crate) struct FetchVanishedConsumer {
    fetches: Vec<FetchResponse>,
    vanished_uids: Vec<UidRange>,
    /// Parsed requested UID set for defensive filtering of
    /// `VANISHED (EARLIER)` responses (RFC 7162 Section 3.2.6).
    /// `None` when the sequence set contains `$` (RFC 5182).
    requested_set: Option<ParsedUidSet>,
    /// Count of individual UIDs dropped by filtering.
    dropped_vanished_count: usize,
    /// Non-solicited responses (classified as `Either`).
    buffered: Vec<UntaggedResponse>,
    /// Running byte-size estimate for the warn-on-large check.
    estimated_bytes: usize,
    warn_threshold: usize,
    warned: bool,
}

impl FetchVanishedConsumer {
    /// Create a new consumer with an optional parsed UID set for
    /// defensive filtering of `VANISHED (EARLIER)` responses.
    ///
    /// Pass `Some(set)` to filter out-of-set UIDs per RFC 7162
    /// Section 3.2.6. Pass `None` when the sequence set contains `$`
    /// (RFC 5182 search result reference) and cannot be parsed.
    pub(crate) fn new(requested_set: Option<ParsedUidSet>) -> Self {
        Self {
            fetches: Vec::new(),
            vanished_uids: Vec::new(),
            requested_set,
            dropped_vanished_count: 0,
            buffered: Vec::new(),
            estimated_bytes: 0,
            warn_threshold: DEFAULT_FETCH_WARN_BYTES,
            warned: false,
        }
    }
}

impl Consumer for FetchVanishedConsumer {
    type Output = (Vec<FetchResponse>, Vec<UidRange>);

    fn on_response(
        &mut self,
        resp: UntaggedResponse,
        _notify_snapshot: NotifyFlags,
        _ctx: &ConsumerContext,
    ) {
        match resp {
            // RFC 7162 Section3.2.6: FETCH responses for messages whose
            // flags changed since the given mod-sequence.
            UntaggedResponse::Fetch(fr) => {
                self.estimated_bytes += estimate_fetch_response_bytes(&fr);
                if !self.warned && self.estimated_bytes > self.warn_threshold {
                    tracing::warn!(
                        estimated_bytes = self.estimated_bytes,
                        threshold = self.warn_threshold,
                        "FETCH response buffer exceeds {} MB; consider \
                         uid_fetch_streaming for large result sets",
                        self.warn_threshold / (1024 * 1024),
                    );
                    self.warned = true;
                }
                self.fetches.push(*fr);
            }
            // RFC 7162 Section3.2.6: VANISHED (EARLIER) lists UIDs expunged
            // since the given mod-sequence. Defensively filter to only
            // include UIDs within the requested set; non-conformant
            // servers may return UIDs outside it.
            UntaggedResponse::Vanished {
                earlier: true,
                uids,
            } => {
                if let Some(ref set) = self.requested_set {
                    let (filtered, dropped) = set.intersect_uid_ranges(&uids);
                    self.dropped_vanished_count += dropped;
                    self.vanished_uids.extend(filtered);
                } else {
                    // No parsed set ($ in sequence set); accept all.
                    self.vanished_uids.extend(uids);
                }
            }
            // Plain VANISHED (earlier: false) and other responses are
            // unsolicited; reclassify as events.
            _ => {
                self.buffered.push(resp);
            }
        }
    }

    fn finalize(
        self: Box<Self>,
        tagged: TaggedResponse,
        _ctx: &ConsumerContext,
    ) -> Result<Finalized<(Vec<FetchResponse>, Vec<UidRange>)>, Error> {
        tagged.require_ok()?;
        if self.dropped_vanished_count > 0 {
            tracing::debug!(
                dropped = self.dropped_vanished_count,
                "filtered out-of-set VANISHED (EARLIER) UIDs per RFC 7162 Section 3.2.6",
            );
        }
        Ok(Finalized {
            output: (self.fetches, self.vanished_uids),
            reclassified_as_events: self.buffered,
        })
    }
}
