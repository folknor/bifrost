//! Higher-level mailbox sync request and result types.

use super::{
    FetchAttr, FetchResponse, ModSeq, QresyncParams, SelectedMailbox, SeqSet, UidSet, UidValidity,
};

/// Options for selecting a mailbox for synchronization.
// protocol-specific: direct IMAP sync helpers carry QRESYNC SELECT operands.
#[non_exhaustive]
#[derive(Debug, Clone, Default, PartialEq, Eq)]
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
    /// Optional sequence-number to UID correspondence for QRESYNC.
    pub seq_match_data: Option<(SeqSet, UidSet)>,
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

    /// Attach QRESYNC sequence-match data.
    ///
    /// RFC 7162 Section 3.2.5.2 allows clients to send
    /// `(known-sequence-set known-uid-set)` in addition to `known-uids` so
    /// the server can detect expunges and renumbering more efficiently.
    pub fn with_qresync_seq_match(mut self, known_seqs: SeqSet, known_uids: UidSet) -> Self {
        self.seq_match_data = Some((known_seqs, known_uids));
        self
    }

    /// Convert to low-level QRESYNC parameters when a complete cursor exists.
    pub fn qresync_params(&self) -> Option<QresyncParams> {
        let uid_validity = self.uid_validity?;
        let mod_seq = self.mod_seq?;
        let mut params = QresyncParams::new(uid_validity.get(), mod_seq.get());
        params.known_uids = self.known_uids.as_ref().map(ToString::to_string);
        params.seq_match_data = self
            .seq_match_data
            .as_ref()
            .map(|(known_seqs, known_uids)| (known_seqs.to_string(), known_uids.to_string()));
        Some(params)
    }
}

/// Result of selecting a mailbox for synchronization.
// protocol-specific: direct IMAP sync helpers expose SELECT extension usage.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyncSelectResult {
    /// Raw selected-mailbox data.
    pub mailbox: SelectedMailbox,
    /// Whether QRESYNC was requested for this SELECT/EXAMINE.
    pub qresync_used: bool,
    /// Whether CONDSTORE was requested for this SELECT/EXAMINE.
    pub condstore_used: bool,
}

/// Common fetch shape for mailbox synchronization.
// protocol-specific: direct IMAP sync helpers carry UID FETCH operands.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
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
// protocol-specific: direct IMAP sync helpers expose FETCH plus VANISHED data.
#[non_exhaustive]
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SyncFetchResult {
    /// Fetched messages.
    pub fetches: Vec<FetchResponse>,
    /// Vanished UID ranges returned by QRESYNC.
    pub vanished: Vec<super::UidRange>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{Seq, Uid};

    #[test]
    fn qresync_params_preserve_seq_match_data() {
        let options = SyncSelectOptions::read_write()
            .with_qresync(
                UidValidity::new(77).expect("valid uidvalidity"),
                ModSeq::new(9000),
                Some(UidSet::range(
                    Uid::new(10).expect("valid uid"),
                    Uid::new(20).expect("valid uid"),
                )),
            )
            .with_qresync_seq_match(
                SeqSet::range(
                    Seq::new(1).expect("valid sequence"),
                    Seq::new(5).expect("valid sequence"),
                ),
                UidSet::range(
                    Uid::new(10).expect("valid uid"),
                    Uid::new(14).expect("valid uid"),
                ),
            );

        let params = options.qresync_params().expect("complete qresync cursor");
        assert_eq!(params.uid_validity, 77);
        assert_eq!(params.mod_seq, 9000);
        assert_eq!(params.known_uids.as_deref(), Some("10:20"));
        assert_eq!(
            params.seq_match_data,
            Some(("1:5".to_owned(), "10:14".to_owned()))
        );
    }
}
