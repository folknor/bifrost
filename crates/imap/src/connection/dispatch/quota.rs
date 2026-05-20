use crate::connection::NotifyFlags;
use crate::connection::helpers::inbox_eq;
use crate::error::Error;
use crate::types::response::{
    AclEntry, ListRightsResponse, MetadataEntry, MetadataResult, QuotaResource, QuotaRootResponse,
    TaggedResponse, UntaggedResponse,
};

use super::{Consumer, ConsumerContext, Finalized};

/// Consumer for GETQUOTA (RFC 2087 Section4.2) and SETQUOTA (RFC 2087 Section4.1).
///
/// Both commands solicit a single untagged QUOTA response for the
/// requested root. Accumulates the first matching QUOTA response;
/// non-matching QUOTA responses are reclassified as events.
pub(crate) struct QuotaConsumer {
    /// The quota root we are looking for.
    root: String,
    /// The matching QUOTA response, if received.
    result: Option<Vec<QuotaResource>>,
    /// Non-matching or non-QUOTA responses routed here via classification.
    buffered: Vec<UntaggedResponse>,
}

impl QuotaConsumer {
    pub(crate) fn new(root: String) -> Self {
        Self {
            root,
            result: None,
            buffered: Vec::new(),
        }
    }
}

impl Consumer for QuotaConsumer {
    type Output = Vec<QuotaResource>;

    fn on_response(
        &mut self,
        resp: UntaggedResponse,
        _notify_snapshot: NotifyFlags,
        _ctx: &ConsumerContext,
    ) {
        // RFC 2087 Section4.2 / Section4.1: accept only the QUOTA for the requested root.
        // RFC 3501 Section5.2: unrelated untagged responses may be interleaved.
        match resp {
            UntaggedResponse::Quota { root, resources }
                if root == self.root && self.result.is_none() =>
            {
                self.result = Some(resources);
            }
            other => self.buffered.push(other),
        }
    }

    fn finalize(
        self: Box<Self>,
        tagged: TaggedResponse,
        _ctx: &ConsumerContext,
    ) -> Result<Finalized<Vec<QuotaResource>>, Error> {
        tagged.require_ok()?;
        let resources = self.result.ok_or_else(|| {
            Error::Protocol(format!(
                "server sent OK but no QUOTA response for root '{}' \
                 (RFC 2087 Section 4.2)",
                self.root,
            ))
        })?;
        Ok(Finalized {
            output: resources,
            reclassified_as_events: self.buffered,
        })
    }
}

/// Consumer for GETQUOTAROOT (RFC 2087 Section4.3 / RFC 9208 Section4.1.2).
///
/// Accumulates the QUOTAROOT response (root names) and all QUOTA
/// responses (resource triplets). Correlates QUOTA responses to the
/// roots listed in the QUOTAROOT response in `finalize`.
pub(crate) struct QuotaRootConsumer {
    /// The mailbox argument, for correlation.
    mailbox: String,
    /// The QUOTAROOT response roots, if received.
    roots: Option<Vec<String>>,
    /// All QUOTA responses received.
    quotas: Vec<(String, Vec<QuotaResource>)>,
    /// Non-matching responses.
    buffered: Vec<UntaggedResponse>,
}

impl QuotaRootConsumer {
    pub(crate) fn new(mailbox: String) -> Self {
        Self {
            mailbox,
            roots: None,
            quotas: Vec::new(),
            buffered: Vec::new(),
        }
    }
}

impl Consumer for QuotaRootConsumer {
    type Output = QuotaRootResponse;

    fn on_response(
        &mut self,
        resp: UntaggedResponse,
        _notify_snapshot: NotifyFlags,
        _ctx: &ConsumerContext,
    ) {
        match resp {
            // RFC 2087 Section4.3: QUOTAROOT response correlates by mailbox
            // via inbox_eq for INBOX case-insensitivity (RFC 3501 Section5.1).
            UntaggedResponse::QuotaRoot { mailbox, roots }
                if inbox_eq(&self.mailbox, mailbox.as_str()) && self.roots.is_none() =>
            {
                self.roots = Some(roots);
            }
            // RFC 2087 Section4.3: QUOTA responses for the returned roots.
            // We consume all QUOTA here. Filtering against the root
            // list happens in finalize, where non-matching ones are
            // reclassified as events.
            UntaggedResponse::Quota { root, resources } => {
                self.quotas.push((root, resources));
            }
            other => self.buffered.push(other),
        }
    }

    fn finalize(
        self: Box<Self>,
        tagged: TaggedResponse,
        _ctx: &ConsumerContext,
    ) -> Result<Finalized<QuotaRootResponse>, Error> {
        tagged.require_ok()?;

        let roots = self.roots.ok_or_else(|| {
            Error::Protocol(format!(
                "server sent OK but no QUOTAROOT response for mailbox '{}' \
                 (RFC 2087 Section 4.3)",
                self.mailbox,
            ))
        })?;

        // Partition QUOTA responses: matching roots are the result,
        // non-matching are reclassified as events.
        let mut resources: Vec<(String, Vec<QuotaResource>)> = Vec::new();
        let mut buffered = self.buffered;
        for (root, res) in self.quotas {
            if roots.iter().any(|expected| expected == &root) {
                resources.push((root, res));
            } else {
                buffered.push(UntaggedResponse::Quota {
                    root,
                    resources: res,
                });
            }
        }

        if roots.is_empty() {
            return Ok(Finalized {
                output: QuotaRootResponse { roots, resources },
                reclassified_as_events: buffered,
            });
        }
        if resources.is_empty() {
            return Err(Error::Protocol(format!(
                "server sent OK but no QUOTA response for QUOTAROOT mailbox \
                 '{}' (RFC 2087 Section 4.3)",
                self.mailbox,
            )));
        }

        Ok(Finalized {
            output: QuotaRootResponse { roots, resources },
            reclassified_as_events: buffered,
        })
    }
}

/// Consumer for GETACL (RFC 4314 Section3.3).
///
/// Accumulates the ACL response for the requested mailbox.
pub(crate) struct AclConsumer {
    /// The mailbox argument, for correlation.
    mailbox: String,
    /// The matching ACL entries, if received.
    result: Option<Vec<AclEntry>>,
    /// Non-matching responses.
    buffered: Vec<UntaggedResponse>,
}

impl AclConsumer {
    pub(crate) fn new(mailbox: String) -> Self {
        Self {
            mailbox,
            result: None,
            buffered: Vec::new(),
        }
    }
}

impl Consumer for AclConsumer {
    type Output = Vec<AclEntry>;

    fn on_response(
        &mut self,
        resp: UntaggedResponse,
        _notify_snapshot: NotifyFlags,
        _ctx: &ConsumerContext,
    ) {
        // RFC 4314 Section3.3 / RFC 3501 Section5.2: correlate by mailbox via
        // inbox_eq for INBOX case-insensitivity.
        match resp {
            UntaggedResponse::Acl { mailbox, entries }
                if inbox_eq(&self.mailbox, mailbox.as_str()) && self.result.is_none() =>
            {
                self.result = Some(entries);
            }
            other => self.buffered.push(other),
        }
    }

    fn finalize(
        self: Box<Self>,
        tagged: TaggedResponse,
        _ctx: &ConsumerContext,
    ) -> Result<Finalized<Vec<AclEntry>>, Error> {
        tagged.require_ok()?;
        let entries = self.result.ok_or_else(|| {
            Error::Protocol(format!(
                "server sent OK but no ACL response for mailbox '{}' \
                 (RFC 4314 Section 3.3)",
                self.mailbox,
            ))
        })?;
        Ok(Finalized {
            output: entries,
            reclassified_as_events: self.buffered,
        })
    }
}

/// Consumer for LISTRIGHTS (RFC 4314 Section3.4).
///
/// Accumulates the LISTRIGHTS response for the requested mailbox and
/// identifier.
pub(crate) struct ListRightsConsumer {
    /// The mailbox argument, for correlation.
    mailbox: String,
    /// The identifier argument, for correlation.
    identifier: String,
    /// The matching LISTRIGHTS response, if received.
    result: Option<ListRightsResponse>,
    /// Non-matching responses.
    buffered: Vec<UntaggedResponse>,
}

impl ListRightsConsumer {
    pub(crate) fn new(mailbox: String, identifier: String) -> Self {
        Self {
            mailbox,
            identifier,
            result: None,
            buffered: Vec::new(),
        }
    }
}

impl Consumer for ListRightsConsumer {
    type Output = ListRightsResponse;

    fn on_response(
        &mut self,
        resp: UntaggedResponse,
        _notify_snapshot: NotifyFlags,
        _ctx: &ConsumerContext,
    ) {
        // RFC 4314 Section3.4: correlate by mailbox AND identifier.
        match resp {
            UntaggedResponse::ListRights {
                mailbox,
                identifier,
                required,
                optional,
            } if inbox_eq(&self.mailbox, mailbox.as_str())
                && identifier == self.identifier
                && self.result.is_none() =>
            {
                self.result = Some(ListRightsResponse { required, optional });
            }
            other => self.buffered.push(other),
        }
    }

    fn finalize(
        self: Box<Self>,
        tagged: TaggedResponse,
        _ctx: &ConsumerContext,
    ) -> Result<Finalized<ListRightsResponse>, Error> {
        tagged.require_ok()?;
        let result = self.result.ok_or_else(|| {
            Error::Protocol(format!(
                "server sent OK but no LISTRIGHTS response for mailbox '{}' \
                 and identifier '{}' (RFC 4314 Section 3.4)",
                self.mailbox, self.identifier,
            ))
        })?;
        Ok(Finalized {
            output: result,
            reclassified_as_events: self.buffered,
        })
    }
}

/// Consumer for MYRIGHTS (RFC 4314 Section3.5).
///
/// Accumulates the MYRIGHTS response for the requested mailbox.
pub(crate) struct MyRightsConsumer {
    /// The mailbox argument, for correlation.
    mailbox: String,
    /// The matching MYRIGHTS rights string, if received.
    result: Option<String>,
    /// Non-matching responses.
    buffered: Vec<UntaggedResponse>,
}

impl MyRightsConsumer {
    pub(crate) fn new(mailbox: String) -> Self {
        Self {
            mailbox,
            result: None,
            buffered: Vec::new(),
        }
    }
}

impl Consumer for MyRightsConsumer {
    type Output = String;

    fn on_response(
        &mut self,
        resp: UntaggedResponse,
        _notify_snapshot: NotifyFlags,
        _ctx: &ConsumerContext,
    ) {
        // RFC 4314 Section3.5: correlate by mailbox.
        match resp {
            UntaggedResponse::MyRights { mailbox, rights }
                if inbox_eq(&self.mailbox, mailbox.as_str()) && self.result.is_none() =>
            {
                self.result = Some(rights);
            }
            other => self.buffered.push(other),
        }
    }

    fn finalize(
        self: Box<Self>,
        tagged: TaggedResponse,
        _ctx: &ConsumerContext,
    ) -> Result<Finalized<String>, Error> {
        tagged.require_ok()?;
        let rights = self.result.ok_or_else(|| {
            Error::Protocol(format!(
                "server sent OK but no MYRIGHTS response for mailbox '{}' \
                 (RFC 4314 Section 3.5)",
                self.mailbox,
            ))
        })?;
        Ok(Finalized {
            output: rights,
            reclassified_as_events: self.buffered,
        })
    }
}

/// Consumer for GETMETADATA (RFC 5464 Section4.2).
///
/// Accumulates same-mailbox METADATA responses and tracks NOTIFY
/// ambiguity via per-response `notify_snapshot`. Different-mailbox
/// METADATA responses are reclassified as events.
///
/// RFC 5465 Section5.6-5.8: when NOTIFY metadata is active, the protocol
/// provides no marker to distinguish solicited METADATA from unsolicited
/// NOTIFY METADATA for the same mailbox. The consumer exposes this via
/// `MetadataResult::notify_ambiguity`.
pub(crate) struct MetadataConsumer {
    /// The mailbox argument, for correlation.
    mailbox: String,
    /// Accumulated metadata entries from same-mailbox responses.
    entries: Vec<MetadataEntry>,
    /// Whether any same-mailbox response arrived while NOTIFY metadata
    /// was active, making the result potentially ambiguous.
    notify_ambiguity: bool,
    /// Whether we saw at least one matching METADATA response.
    saw_matching: bool,
    /// Different-mailbox METADATA and non-METADATA responses.
    buffered: Vec<UntaggedResponse>,
}

impl MetadataConsumer {
    pub(crate) fn new(mailbox: String) -> Self {
        Self {
            mailbox,
            entries: Vec::new(),
            notify_ambiguity: false,
            saw_matching: false,
            buffered: Vec::new(),
        }
    }
}

impl Consumer for MetadataConsumer {
    type Output = MetadataResult;

    fn on_response(
        &mut self,
        resp: UntaggedResponse,
        notify_snapshot: NotifyFlags,
        _ctx: &ConsumerContext,
    ) {
        match resp {
            // Same-mailbox METADATA: accumulate entries.
            // RFC 5464 Section4.2: GETMETADATA can produce multiple METADATA
            // response lines for the same mailbox.
            UntaggedResponse::Metadata { mailbox, entries }
                if inbox_eq(&self.mailbox, mailbox.as_str()) =>
            {
                // RFC 5465 Section5.6-5.8: if NOTIFY metadata was active when
                // this response was generated, the result is ambiguous.
                // Some entries may be from interleaved NOTIFY events.
                // Post-NOTIFICATIONOVERFLOW, apply_side_effects clears the
                // metadata flag, so notify_snapshot.metadata will be false
                // for post-overflow responses (they are unambiguously
                // solicited per RFC 5465 Section5.8).
                if notify_snapshot.metadata {
                    self.notify_ambiguity = true;
                }
                self.saw_matching = true;
                self.entries.extend(entries);
            }
            // Different-mailbox METADATA or non-METADATA: reclassify.
            other => self.buffered.push(other),
        }
    }

    fn finalize(
        self: Box<Self>,
        tagged: TaggedResponse,
        _ctx: &ConsumerContext,
    ) -> Result<Finalized<MetadataResult>, Error> {
        // On failure: drop all matching same-mailbox METADATA rather than
        // buffering as unsolicited. Same-mailbox METADATA is wire-identical
        // between the solicited reply and a NOTIFY event (RFC 5465
        // Section5.6-5.7, RFC 5464 Section4.2). Buffering as unsolicited would
        // leak potentially-solicited data into the NOTIFY event channel.
        tagged.require_ok()?;

        if !self.saw_matching {
            return Err(Error::Protocol(
                "server completed GETMETADATA without the required METADATA \
                 response for the requested mailbox (RFC 5464 Section 4.2)"
                    .into(),
            ));
        }

        Ok(Finalized {
            output: MetadataResult {
                entries: self.entries,
                notify_ambiguity: self.notify_ambiguity,
            },
            reclassified_as_events: self.buffered,
        })
    }
}
