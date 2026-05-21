//! Consumer trait and dispatcher for the new routing architecture.
//!
//! Consumers do NOT route. `classify` routes. Consumers only receive
//! responses that `classify` has already determined are theirs (either
//! solicited or `Either`) and they interpret them.
//!
//! The `Consumer` trait is typed (associated type `Output`).
//! `ConsumerErased` is a blanket-impl wrapper for the pipeline path,
//! which needs `Box<dyn ...>`.

use std::future::Future;
use std::pin::Pin;

use crate::connection::NotifyFlags;
use crate::error::Error;
use crate::types::response::{
    Capability, ContinuationRequest, ResponseCode, TaggedResponse, UntaggedResponse, UntaggedStatus,
};
use crate::types::validated::MailboxName;

mod auth;
mod fetch;
mod list;
mod notify;
mod quota;
mod search;
mod select;
mod thread_sort;

pub(crate) use auth::{
    AuthenticateCramMd5Consumer, AuthenticatePlainConsumer, AuthenticateScramConsumer,
    AuthenticateXoauth2Consumer, LoginConsumer, ScramMechanism,
};
#[cfg(test)]
pub(crate) use fetch::StreamingFetchConsumer;
pub(crate) use fetch::{
    BoundedStreamingFetchConsumer, BoundedStreamingFetchVanishedConsumer, FetchConsumer,
    FetchStreamItem, FetchVanishedConsumer, StoreConsumer,
};
pub(crate) use list::{
    ListConsumer, ListExtendedConsumer, ListStatusConsumer, LsubConsumer, StatusConsumer,
};
pub(crate) use notify::NotifySetConsumer;
pub(crate) use quota::{
    AclConsumer, ListRightsConsumer, MetadataConsumer, MyRightsConsumer, QuotaConsumer,
    QuotaRootConsumer,
};
pub(crate) use search::{
    CopyConsumer, EsearchConsumer, ExpungeConsumer, MoveConsumer, SearchConsumer,
    SearchSaveConsumer,
};
pub(crate) use select::SelectConsumer;
pub(crate) use thread_sort::{SortConsumer, ThreadConsumer};

#[cfg(test)]
use crate::types::FetchResponse;
#[cfg(test)]
use auth::{cram_md5_response, scram_client_final};

/// Typed consumer trait for a single command's response stream.
///
/// Not directly object-safe because `finalize` uses `Self::Output`
/// in its return type. `ConsumerErased` provides the object-safe
/// pipeline path via a blanket impl that erases `Output` to
/// `Box<dyn Any + Send>`.
pub(crate) trait Consumer: Send {
    type Output: Send + 'static;

    /// Called by the dispatcher for each untagged response that
    /// `classify` routed to this command. The response is either
    /// `OnlySolicited` or `Either`  -  the dispatcher never delivers
    /// `OnlyUnsolicited` responses here.
    ///
    /// Consumers accumulate. They do not route.
    fn on_response(
        &mut self,
        resp: UntaggedResponse,
        notify_snapshot: NotifyFlags,
        ctx: &ConsumerContext,
    );

    /// Called when the tagged response arrives. Produces the
    /// command's output and optionally returns responses that the
    /// consumer determined were not actually part of its result (for
    /// `Either` cases  -  the dispatcher re-emits them as events).
    fn finalize(
        self: Box<Self>,
        tagged: TaggedResponse,
        ctx: &ConsumerContext,
    ) -> Result<Finalized<Self::Output>, Error>;
}

/// Current downstream-capacity state for a streaming consumer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BackpressureState {
    /// Consumer can accept the next routed response without awaiting.
    Ready,
    /// Consumer needs the driver to await downstream capacity before
    /// reading more bytes from the wire.
    NeedsCapacity,
    /// Downstream receiver is closed. The driver should keep draining the
    /// command to tagged completion and the consumer will discard data.
    Drained,
}

/// Consumer that can apply async backpressure before the driver reads.
pub(crate) trait StreamingConsumer: Consumer {
    /// Whether the driver should reserve downstream capacity before
    /// reading the next response from the wire.
    fn backpressure_state(&self) -> BackpressureState;

    /// Reserve capacity for the next delivered item.
    fn reserve_capacity(&mut self) -> Pin<Box<dyn Future<Output = Result<(), Error>> + Send + '_>>;
}

/// Output of [`Consumer::finalize`].
pub(crate) struct Finalized<T> {
    /// The command's typed result.
    pub output: T,
    /// Responses the consumer decided were not actually part of its
    /// solicited result. Dispatcher re-emits these to the event sink.
    /// For most consumers this is empty; for consumers that receive
    /// `Either` responses it may contain the responses the consumer
    /// determined were asynchronous notifications.
    pub reclassified_as_events: Vec<UntaggedResponse>,
}

/// Consumer that handles `+` continuations (RFC 3501 Section7.5).
///
/// Used by AUTHENTICATE, APPEND, and future multi-round SASL.
/// The dispatcher routes continuations to `on_continuation` instead
/// of erroring on unexpected `+`.
pub(crate) trait ContinuationConsumer: Consumer {
    /// Handle a `+` continuation from the server.
    ///
    /// Returns either bytes to write back to the wire, or an abort
    /// signal with an error.
    fn on_continuation(
        &mut self,
        cont: ContinuationRequest,
        ctx: &ConsumerContext,
    ) -> Result<ContinuationReply, Error>;
}

/// What to do after a `+` continuation is delivered to a
/// [`ContinuationConsumer`].
pub(crate) enum ContinuationReply {
    /// Write these bytes to the wire and continue reading.
    Write(Vec<u8>),
}

/// Read-only view of the connection state the consumer needs.
///
/// Exposes only the fields a consumer is allowed to observe; does
/// NOT expose a reference to `ProtocolState` itself (which would
/// leak the private state module's shape).
pub(crate) struct ConsumerContext<'a> {
    // All fields are pub(in crate::connection)  -  constructed by the
    // dispatcher inside the connection module, read by consumers
    // via the accessor methods below. No field is pub.
    pub(in crate::connection) capabilities: &'a [Capability],
    pub(in crate::connection) enabled: &'a [String],
    pub(in crate::connection) command_target: Option<&'a MailboxName>,
    /// The tag of the in-flight command. Used by consumers that need
    /// to correlate solicited responses (e.g., ESEARCH tag correlation
    /// per RFC 4466 search-correlator).
    pub(in crate::connection) command_tag: &'a str,
}

impl ConsumerContext<'_> {
    /// Cached server capabilities (RFC 3501 Section7.2.1).
    pub(crate) fn capabilities(&self) -> &[Capability] {
        self.capabilities
    }

    /// Successfully `ENABLE`d extensions (RFC 5161 Section3.2).
    pub(crate) fn enabled(&self) -> &[String] {
        self.enabled
    }

    /// The mailbox argument of the current command, if applicable.
    pub(crate) fn command_target(&self) -> Option<&MailboxName> {
        self.command_target
    }

    /// The tag of the in-flight command (RFC 3501 Section2.2.1).
    pub(crate) fn command_tag(&self) -> &str {
        self.command_tag
    }
}

// ---------------------------------------------------------------------------
// Consumers  -  NOOP / CAPABILITY / CHECK / ENABLE / NAMESPACE / IDLE
// ---------------------------------------------------------------------------

/// Consumer for commands that expect no solicited untagged data.
///
/// Validates the tagged response is OK and reclassifies all untagged
/// responses it received as events (they were `Either`  -  ambiguous
/// between solicited and async, but this command has no use for them).
///
/// Used by NOOP (RFC 3501 Section6.1.2), DELETE (RFC 3501 Section6.3.4),
/// RENAME (RFC 3501 Section6.3.5), SUBSCRIBE (RFC 3501 Section6.3.6),
/// UNSUBSCRIBE (RFC 3501 Section6.3.7), and similar.
#[derive(Default)]
pub(crate) struct TaggedOkConsumer {
    buffered: Vec<UntaggedResponse>,
}

impl Consumer for TaggedOkConsumer {
    type Output = ();

    fn on_response(
        &mut self,
        resp: UntaggedResponse,
        _notify_snapshot: NotifyFlags,
        _ctx: &ConsumerContext,
    ) {
        // Commands that use TaggedOkConsumer produce no untagged data
        // of their own. Every response `classify` routes here is Either
        // (async state changes). Buffer and reclassify in finalize.
        self.buffered.push(resp);
    }

    fn finalize(
        self: Box<Self>,
        tagged: TaggedResponse,
        _ctx: &ConsumerContext,
    ) -> Result<Finalized<()>, Error> {
        tagged.require_ok()?;
        Ok(Finalized {
            output: (),
            reclassified_as_events: self.buffered,
        })
    }
}

/// Consumer for CAPABILITY (RFC 3501 Section6.1.1).
///
/// Accumulates the untagged CAPABILITY response. If the server places
/// capabilities in the tagged OK response code instead (permitted by
/// RFC 3501 Section6.1.1), finalize extracts them from there.
#[derive(Default)]
pub(crate) struct CapabilityConsumer {
    caps: Option<Vec<Capability>>,
    buffered: Vec<UntaggedResponse>,
}

impl Consumer for CapabilityConsumer {
    type Output = Vec<Capability>;

    fn on_response(
        &mut self,
        resp: UntaggedResponse,
        _notify_snapshot: NotifyFlags,
        _ctx: &ConsumerContext,
    ) {
        // RFC 3501 Section6.1.1: the server MUST respond with a CAPABILITY
        // untagged response. Stash it; reclassify everything else.
        if let UntaggedResponse::Capability(ref c) = resp {
            self.caps = Some(c.clone());
        } else {
            self.buffered.push(resp);
        }
    }

    fn finalize(
        self: Box<Self>,
        tagged: TaggedResponse,
        _ctx: &ConsumerContext,
    ) -> Result<Finalized<Vec<Capability>>, Error> {
        let tagged = tagged.require_ok()?;

        // RFC 3501 Section6.1.1: capabilities may appear as an untagged
        // response or in the tagged OK response code.
        let caps = if let Some(c) = self.caps {
            c
        } else if let Some(ResponseCode::Capability(c)) = tagged.code {
            c
        } else {
            return Err(Error::Protocol(
                "CAPABILITY OK but no capability data in response \
                 (RFC 3501 Section 6.1.1)"
                    .into(),
            ));
        };

        Ok(Finalized {
            output: caps,
            reclassified_as_events: self.buffered,
        })
    }
}

/// Consumer for LOGOUT (RFC 3501 Section6.1.3).
///
/// Tracks whether the mandatory `* BYE` response was received.
/// RFC 3501 Section6.1.3: the server MUST send `* BYE` before the tagged OK.
#[derive(Default)]
pub(crate) struct LogoutConsumer {
    saw_bye: bool,
    buffered: Vec<UntaggedResponse>,
}

impl Consumer for LogoutConsumer {
    type Output = ();

    fn on_response(
        &mut self,
        resp: UntaggedResponse,
        _notify_snapshot: NotifyFlags,
        _ctx: &ConsumerContext,
    ) {
        // RFC 3501 Section6.1.3: the server MUST send `* BYE` before the
        // tagged OK response to LOGOUT.
        if matches!(
            &resp,
            UntaggedResponse::Status {
                status: UntaggedStatus::Bye,
                ..
            }
        ) {
            self.saw_bye = true;
        }
        self.buffered.push(resp);
    }

    fn finalize(
        self: Box<Self>,
        tagged: TaggedResponse,
        _ctx: &ConsumerContext,
    ) -> Result<Finalized<()>, Error> {
        // Check BYE first  -  if the server omitted it, that is a protocol
        // error even when the tagged status is OK.
        if !self.saw_bye {
            return Err(Error::Protocol(
                "LOGOUT: server did not send mandatory BYE \
                 (RFC 3501 Section 6.1.3)"
                    .into(),
            ));
        }
        tagged.require_ok()?;
        Ok(Finalized {
            output: (),
            reclassified_as_events: self.buffered,
        })
    }
}

/// Consumer for CREATE (RFC 3501 Section6.3.3) and CREATE-SPECIAL-USE (RFC 6154 Section3).
///
/// Extracts the optional `MAILBOXID` response code from the tagged OK
/// (RFC 8474 Section4.1). Servers advertising `OBJECTID` MUST include it;
/// others may omit it.
#[derive(Default)]
pub(crate) struct CreateConsumer {
    buffered: Vec<UntaggedResponse>,
}

impl Consumer for CreateConsumer {
    type Output = Option<String>;

    fn on_response(
        &mut self,
        resp: UntaggedResponse,
        _notify_snapshot: NotifyFlags,
        _ctx: &ConsumerContext,
    ) {
        // CREATE has no untagged responses of its own. Buffer
        // everything for reclassification.
        self.buffered.push(resp);
    }

    fn finalize(
        self: Box<Self>,
        tagged: TaggedResponse,
        _ctx: &ConsumerContext,
    ) -> Result<Finalized<Option<String>>, Error> {
        let tagged = tagged.require_ok()?;
        // RFC 8474 Section4.1: MAILBOXID in the tagged OK response code.
        let mailbox_id = match tagged.code {
            Some(ResponseCode::MailboxId(id)) => Some(id),
            _ => None,
        };
        Ok(Finalized {
            output: mailbox_id,
            reclassified_as_events: self.buffered,
        })
    }
}

// ---------------------------------------------------------------------------
// Consumers  -  APPEND / MULTIAPPEND
// ---------------------------------------------------------------------------

/// Consumer for APPEND (RFC 3501 Section6.3.11).
///
/// APPEND has no solicited untagged responses of its own  -  all
/// untagged data during APPEND is async state changes (EXISTS,
/// EXPUNGE, FETCH, etc.). The result is extracted from the tagged
/// OK response code: `[APPENDUID uidvalidity uid]` (RFC 4315 Section3).
#[derive(Default)]
pub(crate) struct AppendConsumer {
    buffered: Vec<UntaggedResponse>,
    /// APPENDUID response code extracted from an untagged `* OK [APPENDUID ...]`.
    /// Some servers send APPENDUID in an untagged OK rather than in the
    /// tagged OK (RFC 4315 Section3).
    code: Option<ResponseCode>,
}

impl Consumer for AppendConsumer {
    type Output = Option<(u32, u32)>;

    fn on_response(
        &mut self,
        resp: UntaggedResponse,
        _notify_snapshot: NotifyFlags,
        _ctx: &ConsumerContext,
    ) {
        // APPEND produces no untagged responses of its own
        // (RFC 3501 Section6.3.11). Buffer everything for reclassification.
        match resp {
            // RFC 4315 Section3: some servers send APPENDUID in an untagged OK.
            UntaggedResponse::Status {
                status: UntaggedStatus::Ok,
                code: code_opt @ Some(ResponseCode::AppendUid { .. }),
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
    ) -> Result<Finalized<Option<(u32, u32)>>, Error> {
        let tagged = tagged.require_ok()?;
        // RFC 4315 Section3: extract APPENDUID from the tagged OK response code.
        // Servers without UIDPLUS may omit it.
        let code = tagged.code.or(self.code);
        let append_uid = match code {
            Some(ResponseCode::AppendUid { uid_validity, uids }) => {
                // Single APPEND  -  extract the first UID from the set.
                uids.first().map(|r| (uid_validity, r.start))
            }
            _ => None,
        };
        Ok(Finalized {
            output: append_uid,
            reclassified_as_events: self.buffered,
        })
    }
}

/// Consumer for MULTIAPPEND (RFC 3502).
///
/// Same as [`AppendConsumer`] but extracts multiple UIDs from the
/// `[APPENDUID]` response code. Each UID range is expanded into
/// individual `(uid_validity, uid)` pairs.
#[derive(Default)]
pub(crate) struct MultiAppendConsumer {
    buffered: Vec<UntaggedResponse>,
    /// APPENDUID response code extracted from an untagged `* OK [APPENDUID ...]`.
    /// Some servers send APPENDUID in an untagged OK rather than in the
    /// tagged OK (RFC 4315 Section3).
    code: Option<ResponseCode>,
}

impl Consumer for MultiAppendConsumer {
    type Output = Vec<(u32, u32)>;

    fn on_response(
        &mut self,
        resp: UntaggedResponse,
        _notify_snapshot: NotifyFlags,
        _ctx: &ConsumerContext,
    ) {
        // MULTIAPPEND produces no untagged responses of its own
        // (RFC 3502 Section3). Buffer everything for reclassification.
        match resp {
            // RFC 4315 Section3: some servers send APPENDUID in an untagged OK.
            UntaggedResponse::Status {
                status: UntaggedStatus::Ok,
                code: code_opt @ Some(ResponseCode::AppendUid { .. }),
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
    ) -> Result<Finalized<Vec<(u32, u32)>>, Error> {
        let tagged = tagged.require_ok()?;
        // RFC 4315 Section3: for MULTIAPPEND, the uid-set contains one
        // UID per appended message, possibly as ranges.
        let mut results = Vec::new();
        let code = tagged.code.or(self.code);
        if let Some(ResponseCode::AppendUid { uid_validity, uids }) = code {
            for range in &uids {
                if let Some(end) = range.end {
                    // Expand range into individual (uid_validity, uid) pairs.
                    for uid in range.start..=end {
                        results.push((uid_validity, uid));
                    }
                } else {
                    results.push((uid_validity, range.start));
                }
            }
        }
        Ok(Finalized {
            output: results,
            reclassified_as_events: self.buffered,
        })
    }
}

// ---------------------------------------------------------------------------
// Consumers  -  ID / COMPRESS / STARTTLS / LOGOUT
// ---------------------------------------------------------------------------

/// Consumer for ID (RFC 2971 Section3.1).
///
/// Extracts the server's identity key-value pairs from the untagged
/// ID response. RFC 2971 Section3.2: the server MUST respond with an ID response.
#[derive(Default)]
pub(crate) struct IdConsumer {
    pairs: Option<Vec<(String, Option<String>)>>,
    buffered: Vec<UntaggedResponse>,
}

impl Consumer for IdConsumer {
    type Output = Vec<(String, Option<String>)>;

    fn on_response(
        &mut self,
        resp: UntaggedResponse,
        _notify_snapshot: NotifyFlags,
        _ctx: &ConsumerContext,
    ) {
        // RFC 2971 Section3.2: take the first ID response.
        match resp {
            UntaggedResponse::Id(pairs) if self.pairs.is_none() => {
                self.pairs = Some(pairs);
            }
            other => self.buffered.push(other),
        }
    }

    fn finalize(
        self: Box<Self>,
        tagged: TaggedResponse,
        _ctx: &ConsumerContext,
    ) -> Result<Finalized<Vec<(String, Option<String>)>>, Error> {
        tagged.require_ok()?;
        let pairs = self.pairs.ok_or_else(|| {
            Error::Protocol("ID OK but no untagged ID response (RFC 2971 Section 3.2)".into())
        })?;
        Ok(Finalized {
            output: pairs,
            reclassified_as_events: self.buffered,
        })
    }
}

/// Consumer for NAMESPACE (RFC 2342 Section5).
///
/// Extracts the personal, other-users, and shared namespace
/// descriptors from the untagged NAMESPACE response.
#[derive(Default)]
pub(crate) struct NamespaceConsumer {
    namespace: Option<(
        Vec<crate::types::NamespaceDescriptor>,
        Vec<crate::types::NamespaceDescriptor>,
        Vec<crate::types::NamespaceDescriptor>,
    )>,
    buffered: Vec<UntaggedResponse>,
}

impl Consumer for NamespaceConsumer {
    type Output = crate::types::NamespaceResponse;

    fn on_response(
        &mut self,
        resp: UntaggedResponse,
        _notify_snapshot: NotifyFlags,
        _ctx: &ConsumerContext,
    ) {
        // RFC 2342 Section5: take the first NAMESPACE response.
        match resp {
            UntaggedResponse::Namespace {
                personal,
                other,
                shared,
            } if self.namespace.is_none() => {
                self.namespace = Some((personal, other, shared));
            }
            other => self.buffered.push(other),
        }
    }

    fn finalize(
        self: Box<Self>,
        tagged: TaggedResponse,
        _ctx: &ConsumerContext,
    ) -> Result<Finalized<crate::types::NamespaceResponse>, Error> {
        tagged.require_ok()?;
        let (personal, other, shared) = self.namespace.ok_or_else(|| {
            Error::Protocol(
                "NAMESPACE OK but no untagged NAMESPACE response (RFC 2342 Section 5)".into(),
            )
        })?;
        Ok(Finalized {
            output: crate::types::NamespaceResponse {
                personal,
                other,
                shared,
            },
            reclassified_as_events: self.buffered,
        })
    }
}

/// Consumer for the ENABLE command (RFC 5161 Section 3).
///
/// Captures the `* ENABLED` untagged response and returns the list
/// of extensions the server actually enabled for this request.
#[derive(Default)]
pub(crate) struct EnableConsumer {
    /// The enabled extensions from `* ENABLED`.
    caps: Option<Vec<String>>,
    /// Non-matching untagged responses to reclassify as events.
    buffered: Vec<UntaggedResponse>,
}

impl Consumer for EnableConsumer {
    type Output = Vec<String>;

    fn on_response(
        &mut self,
        resp: UntaggedResponse,
        _notify_snapshot: NotifyFlags,
        _ctx: &ConsumerContext,
    ) {
        match resp {
            UntaggedResponse::Enabled(exts) if self.caps.is_none() => {
                self.caps = Some(exts);
            }
            other => self.buffered.push(other),
        }
    }

    fn finalize(
        self: Box<Self>,
        tagged: TaggedResponse,
        _ctx: &ConsumerContext,
    ) -> Result<Finalized<Vec<String>>, Error> {
        tagged.require_ok()?;
        // RFC 5161 Section 3.2: the server MUST send an ENABLED response.
        // Tolerate omission per Postel's law  -  warn and return empty.
        let exts = self.caps.unwrap_or_else(|| {
            tracing::warn!(
                "server omitted ENABLED response (RFC 5161 Section 3.2) \
                  -  treating as empty"
            );
            Vec::new()
        });
        Ok(Finalized {
            output: exts,
            reclassified_as_events: self.buffered,
        })
    }
}

#[cfg(test)]
#[path = "dispatch_tests.rs"]
mod tests;
