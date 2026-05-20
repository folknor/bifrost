//! Higher-level mailbox sync request and result types.

use super::{
    FetchAttr, FetchResponse, ModSeq, QresyncParams, SelectedMailbox, UidSet, UidValidity,
};

/// Options for selecting a mailbox for synchronization.
#[non_exhaustive]
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct SyncSelectOptions {
    /// Open the mailbox read-only via EXAMINE.
    pub read_only: bool,
    /// Enable CONDSTORE when available.
    pub condstore: bool,
    /// Last known UIDVALIDITY for QRESYNC.
    pub uid_validity: Option<UidValidity>,
    /// Last known highest MODSEQ for QRESYNC.
    pub mod_seq: Option<ModSeq>,
    /// Optional known UID set for QRESYNC.
    pub known_uids: Option<UidSet>,
}

impl SyncSelectOptions {
    /// Request a read-write SELECT with the best available sync extension.
    pub fn read_write() -> Self {
        Self {
            condstore: true,
            ..Self::default()
        }
    }

    /// Request read-only EXAMINE with the best available sync extension.
    pub fn read_only() -> Self {
        Self {
            read_only: true,
            condstore: true,
            ..Self::default()
        }
    }

    /// Attach a QRESYNC cursor.
    pub fn with_qresync(
        mut self,
        uid_validity: UidValidity,
        mod_seq: ModSeq,
        known_uids: Option<UidSet>,
    ) -> Self {
        self.uid_validity = Some(uid_validity);
        self.mod_seq = Some(mod_seq);
        self.known_uids = known_uids;
        self
    }

    /// Convert to low-level QRESYNC parameters when a complete cursor exists.
    pub fn qresync_params(&self) -> Option<QresyncParams> {
        let uid_validity = self.uid_validity?;
        let mod_seq = self.mod_seq?;
        let mut params = QresyncParams::new(uid_validity.get(), mod_seq.get());
        params.known_uids = self.known_uids.as_ref().map(ToString::to_string);
        Some(params)
    }
}

/// Result of selecting a mailbox for synchronization.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct SyncSelectResult {
    /// Raw selected-mailbox data.
    pub mailbox: SelectedMailbox,
    /// Whether QRESYNC was requested for this SELECT/EXAMINE.
    pub qresync_used: bool,
    /// Whether CONDSTORE was requested for this SELECT/EXAMINE.
    pub condstore_used: bool,
}

/// Common fetch shape for mailbox synchronization.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct SyncFetchRequest {
    /// UID set to fetch.
    pub uids: UidSet,
    /// Attributes to request.
    pub attrs: Vec<FetchAttr>,
    /// Optional CHANGEDSINCE value.
    pub changed_since: Option<ModSeq>,
    /// Request `VANISHED` with `CHANGEDSINCE` when QRESYNC is enabled.
    pub include_vanished: bool,
}

impl SyncFetchRequest {
    /// Fetch flags and UID for all messages.
    pub fn flags() -> Self {
        Self {
            uids: UidSet::all(),
            attrs: vec![FetchAttr::Uid, FetchAttr::Flags, FetchAttr::ModSeq],
            changed_since: None,
            include_vanished: false,
        }
    }

    /// Fetch envelope-sized metadata for all messages.
    pub fn envelope() -> Self {
        Self {
            uids: UidSet::all(),
            attrs: vec![
                FetchAttr::Uid,
                FetchAttr::Flags,
                FetchAttr::Envelope,
                FetchAttr::InternalDate,
                FetchAttr::Rfc822Size,
                FetchAttr::ModSeq,
            ],
            changed_since: None,
            include_vanished: false,
        }
    }

    /// Fetch full RFC 5322 message bytes for the given UID set without
    /// setting `\Seen`.
    pub fn full_messages(uids: UidSet) -> Self {
        Self {
            uids,
            attrs: vec![
                FetchAttr::Uid,
                FetchAttr::Flags,
                FetchAttr::Rfc822Size,
                FetchAttr::BodySection {
                    peek: true,
                    section: None,
                    partial: None,
                },
            ],
            changed_since: None,
            include_vanished: false,
        }
    }

    /// Add CHANGEDSINCE to the request.
    pub fn changed_since(mut self, mod_seq: ModSeq) -> Self {
        self.changed_since = Some(mod_seq);
        self
    }

    /// Request VANISHED data for a QRESYNC delta fetch.
    pub fn include_vanished(mut self) -> Self {
        self.include_vanished = true;
        self
    }
}

/// Result of a sync fetch.
#[non_exhaustive]
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct SyncFetchResult {
    /// Fetched messages.
    pub fetches: Vec<FetchResponse>,
    /// Vanished UID ranges returned by QRESYNC.
    pub vanished: Vec<super::UidRange>,
}
