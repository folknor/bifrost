use bifrost_types::{
    AccountError, AccountErrorBuilder, AccountErrorKind, BlobId, Cause, ChangeCursor, CursorScope,
    DiagnosticText, ObjectId, OpaqueChangeState, Protocol, ProtocolKind, RequestCause,
    RequestErrorKind, SyncStateErrorKind, ThreadId,
};

use crate::types::MailboxName;

use super::CompactUidSet;

pub(crate) const ENVELOPE_VERSION: u32 = 1;
const MAGIC: &[u8; 8] = b"IMAPCUR1";

// protocol-specific: this is the IMAP payload carried inside OpaqueChangeState.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum FolderCursor {
    QResync {
        uidvalidity: u32,
        modseq: u64,
        known_uids: CompactUidSet,
        known_uids_complete: bool,
    },
    Condstore {
        uidvalidity: u32,
        modseq: u64,
        known_uids: CompactUidSet,
    },
    Basic {
        uidvalidity: u32,
        uidnext: u32,
        known_uids: CompactUidSet,
    },
}

impl FolderCursor {
    pub(crate) fn uidvalidity(&self) -> u32 {
        match self {
            Self::QResync { uidvalidity, .. }
            | Self::Condstore { uidvalidity, .. }
            | Self::Basic { uidvalidity, .. } => *uidvalidity,
        }
    }

    pub(crate) fn known_uids(&self) -> &CompactUidSet {
        match self {
            Self::QResync { known_uids, .. }
            | Self::Condstore { known_uids, .. }
            | Self::Basic { known_uids, .. } => known_uids,
        }
    }
}

pub(crate) fn encode_cursor(scope: CursorScope, cursor: &FolderCursor) -> ChangeCursor {
    ChangeCursor {
        scope,
        server_state: OpaqueChangeState {
            protocol: ProtocolKind::Imap,
            envelope_version: ENVELOPE_VERSION,
            bytes: encode_folder_cursor(cursor),
        },
        advanced_through: None,
        envelope_version: ENVELOPE_VERSION,
    }
}

pub(crate) fn decode_cursor(cursor: &ChangeCursor) -> Result<FolderCursor, AccountError> {
    if cursor.server_state.protocol != ProtocolKind::Imap {
        return Err(schema_incompatible(
            "cursor protocol mismatch: expected IMAP",
        ));
    }
    if cursor.server_state.envelope_version != ENVELOPE_VERSION {
        return Err(schema_incompatible(&format!(
            "unsupported IMAP cursor envelope version {}",
            cursor.server_state.envelope_version
        )));
    }
    decode_folder_cursor(&cursor.server_state.bytes)
}

fn encode_folder_cursor(cursor: &FolderCursor) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(MAGIC);
    match cursor {
        FolderCursor::QResync {
            uidvalidity,
            modseq,
            known_uids,
            known_uids_complete,
        } => {
            out.push(1);
            push_u32(&mut out, *uidvalidity);
            push_u64(&mut out, *modseq);
            encode_uid_set(&mut out, known_uids);
            out.push(u8::from(*known_uids_complete));
        }
        FolderCursor::Condstore {
            uidvalidity,
            modseq,
            known_uids,
        } => {
            out.push(2);
            push_u32(&mut out, *uidvalidity);
            push_u64(&mut out, *modseq);
            encode_uid_set(&mut out, known_uids);
        }
        FolderCursor::Basic {
            uidvalidity,
            uidnext,
            known_uids,
        } => {
            out.push(3);
            push_u32(&mut out, *uidvalidity);
            push_u32(&mut out, *uidnext);
            encode_uid_set(&mut out, known_uids);
        }
    }
    out
}

fn decode_folder_cursor(bytes: &[u8]) -> Result<FolderCursor, AccountError> {
    let mut input = CursorBytes::new(bytes);
    input.expect_magic()?;
    let tag = input.take_u8()?;
    match tag {
        1 => {
            let uidvalidity = input.take_u32()?;
            let modseq = input.take_u64()?;
            // Three on-disk shapes are accepted:
            //   * pre-baseline v1: no bytes after modseq -> empty set,
            //     incomplete (legacy cursors written before known_uids
            //     was tracked at all)
            //   * mid-PR shape: uid set bytes only, no completeness
            //     byte -> assume complete (the only writer that emits
            //     this shape hardcodes `complete: true`)
            //   * current shape: uid set bytes followed by a u8 flag
            let (known_uids, known_uids_complete) = if input.remaining() == 0 {
                (CompactUidSet::default(), false)
            } else {
                let set = input.take_uid_set()?;
                let complete = if input.remaining() == 0 {
                    true
                } else {
                    input.take_u8()? != 0
                };
                (set, complete)
            };
            Ok(FolderCursor::QResync {
                uidvalidity,
                modseq,
                known_uids,
                known_uids_complete,
            })
        }
        2 => Ok(FolderCursor::Condstore {
            uidvalidity: input.take_u32()?,
            modseq: input.take_u64()?,
            known_uids: input.take_uid_set()?,
        }),
        3 => Ok(FolderCursor::Basic {
            uidvalidity: input.take_u32()?,
            uidnext: input.take_u32()?,
            known_uids: input.take_uid_set()?,
        }),
        _ => Err(schema_incompatible("unknown IMAP cursor tag")),
    }
}

fn encode_uid_set(out: &mut Vec<u8>, set: &CompactUidSet) {
    push_u32(out, u32::try_from(set.ranges().len()).unwrap_or(u32::MAX));
    for range in set.ranges() {
        push_u32(out, range.start);
        push_u32(out, range.end.unwrap_or(0));
    }
}

fn push_u32(out: &mut Vec<u8>, value: u32) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn push_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_le_bytes());
}

struct CursorBytes<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> CursorBytes<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn expect_magic(&mut self) -> Result<(), AccountError> {
        let magic = self.take(MAGIC.len())?;
        if magic == MAGIC {
            Ok(())
        } else {
            Err(schema_incompatible("IMAP cursor magic bytes did not match"))
        }
    }

    fn take(&mut self, count: usize) -> Result<&'a [u8], AccountError> {
        let end = self
            .offset
            .checked_add(count)
            .ok_or_else(|| schema_incompatible("IMAP cursor byte offset overflow"))?;
        let out = self
            .bytes
            .get(self.offset..end)
            .ok_or_else(|| schema_incompatible("IMAP cursor data truncated"))?;
        self.offset = end;
        Ok(out)
    }

    fn remaining(&self) -> usize {
        self.bytes.len().saturating_sub(self.offset)
    }

    fn take_u8(&mut self) -> Result<u8, AccountError> {
        self.take(1).map(|b| b[0])
    }

    fn take_u32(&mut self) -> Result<u32, AccountError> {
        let bytes: [u8; 4] = self
            .take(4)?
            .try_into()
            .map_err(|_| schema_incompatible("IMAP cursor u32 field malformed"))?;
        Ok(u32::from_le_bytes(bytes))
    }

    fn take_u64(&mut self) -> Result<u64, AccountError> {
        let bytes: [u8; 8] = self
            .take(8)?
            .try_into()
            .map_err(|_| schema_incompatible("IMAP cursor u64 field malformed"))?;
        Ok(u64::from_le_bytes(bytes))
    }

    fn take_uid_set(&mut self) -> Result<CompactUidSet, AccountError> {
        let count = self.take_u32()? as usize;
        let mut ranges = Vec::with_capacity(count);
        for _ in 0..count {
            let start = self.take_u32()?;
            let end = self.take_u32()?;
            // `end == 0` is the single-UID sentinel (UIDs are nz-number,
            // so 0 can never be a real range end). Any other `end < start`
            // is a genuinely malformed range that would otherwise build a
            // backwards `UidRange`; reject it rather than silently decode
            // a corrupt cursor into a nonsensical set.
            let range = if end == 0 {
                crate::types::UidRange::single(start)
            } else if end < start {
                return Err(schema_incompatible(
                    "IMAP cursor UID range has end before start",
                ));
            } else {
                crate::types::UidRange::range(start, end)
            };
            ranges.push(range);
        }
        Ok(CompactUidSet::from_ranges(ranges))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DecodedObjectId {
    pub(crate) folder: MailboxName,
    pub(crate) uidvalidity: u32,
    pub(crate) uid: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DecodedBlobId {
    pub(crate) folder: MailboxName,
    pub(crate) uidvalidity: u32,
    pub(crate) uid: u32,
    pub(crate) section: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DecodedThreadId {
    pub(crate) folder: MailboxName,
    pub(crate) uidvalidity: u32,
    pub(crate) uids: Vec<u32>,
}

pub(crate) fn encode_object_id(folder: &MailboxName, uidvalidity: u32, uid: u32) -> ObjectId {
    ObjectId(format!(
        "imap1:{}:{}:{}:{}",
        folder.as_str().len(),
        folder.as_str(),
        uidvalidity,
        uid
    ))
}

pub(crate) fn decode_object_id(id: &ObjectId) -> Result<DecodedObjectId, AccountError> {
    let (folder, rest) = decode_len_prefixed("imap1", &id.0)?;
    let mut parts = rest.split(':');
    let uidvalidity = parse_u32(parts.next())?;
    let uid = parse_u32(parts.next())?;
    if parts.next().is_some() {
        return Err(malformed("invalid IMAP object id"));
    }
    Ok(DecodedObjectId {
        folder,
        uidvalidity,
        uid,
    })
}

#[cfg(test)]
pub(crate) fn encode_blob_id(
    folder: &MailboxName,
    uidvalidity: u32,
    uid: u32,
    section: Option<&str>,
) -> BlobId {
    let section = section.unwrap_or("");
    BlobId(format!(
        "imapblob1:{}:{}:{}:{}:{}",
        folder.as_str().len(),
        folder.as_str(),
        uidvalidity,
        uid,
        section
    ))
}

pub(crate) fn decode_blob_id(id: &BlobId) -> Result<DecodedBlobId, AccountError> {
    let (folder, rest) = decode_len_prefixed("imapblob1", &id.0)?;
    let mut parts = rest.splitn(3, ':');
    let uidvalidity = parse_u32(parts.next())?;
    let uid = parse_u32(parts.next())?;
    let section = parts
        .next()
        .filter(|value| !value.is_empty())
        .map(str::to_owned);
    Ok(DecodedBlobId {
        folder,
        uidvalidity,
        uid,
        section,
    })
}

pub(crate) fn encode_thread_id(folder: &MailboxName, uidvalidity: u32, uids: &[u32]) -> ThreadId {
    let mut uids = uids.to_vec();
    uids.sort_unstable();
    uids.dedup();
    let uid_list = uids
        .iter()
        .map(std::string::ToString::to_string)
        .collect::<Vec<_>>()
        .join(",");
    ThreadId(format!(
        "imapthread1:{}:{}:{}:{}",
        folder.as_str().len(),
        folder.as_str(),
        uidvalidity,
        uid_list
    ))
}

pub(crate) fn decode_thread_id(id: &ThreadId) -> Result<DecodedThreadId, AccountError> {
    let (folder, rest) = decode_len_prefixed("imapthread1", &id.0)?;
    let mut parts = rest.splitn(2, ':');
    let uidvalidity = parse_u32(parts.next())?;
    let uid_part = parts
        .next()
        .ok_or_else(|| malformed("missing IMAP thread uid set"))?;
    let mut uids = Vec::new();
    for uid in uid_part.split(',').filter(|part| !part.is_empty()) {
        uids.push(
            uid.parse::<u32>()
                .map_err(|_| malformed("invalid IMAP thread uid"))?,
        );
    }
    if uids.is_empty() {
        return Err(malformed("empty IMAP thread uid set"));
    }
    Ok(DecodedThreadId {
        folder,
        uidvalidity,
        uids,
    })
}

fn decode_len_prefixed(prefix: &str, value: &str) -> Result<(MailboxName, String), AccountError> {
    let value = value
        .strip_prefix(prefix)
        .and_then(|v| v.strip_prefix(':'))
        .ok_or_else(|| malformed("invalid IMAP id prefix"))?;
    let Some((len, rest)) = value.split_once(':') else {
        return Err(malformed("invalid IMAP id length"));
    };
    let len = len
        .parse::<usize>()
        .map_err(|_| malformed("invalid IMAP id length"))?;
    let folder = rest
        .get(..len)
        .ok_or_else(|| malformed("invalid IMAP id folder length"))?;
    let after = rest
        .get(len..)
        .and_then(|v| v.strip_prefix(':'))
        .ok_or_else(|| malformed("invalid IMAP id separator"))?;
    let folder = MailboxName::new(folder.to_owned()).map_err(|e| malformed(&e.to_string()))?;
    Ok((folder, after.to_owned()))
}

fn parse_u32(value: Option<&str>) -> Result<u32, AccountError> {
    value
        .ok_or_else(|| malformed("missing IMAP id field"))?
        .parse::<u32>()
        .map_err(|_| malformed("invalid IMAP id integer"))
}

/// Build a `Request(Malformed)` `AccountError` for IMAP object-id or
/// cursor decode failures. These are caller-side invalid inputs (the
/// object id was not produced by this crate or was corrupted in transit).
///
/// Operation is left unset here because malformed-id detection happens
/// outside any specific account-trait call site. Callers that decode
/// an id while servicing a specific operation thread the operation via
/// their own `ImapErrorContext`; this builder is only reached for
/// `decode_object_id` / `decode_cursor` from contexts where the op
/// isn't known.
fn malformed(detail: &str) -> AccountError {
    AccountErrorBuilder::new(
        AccountErrorKind::Request(RequestErrorKind::Malformed),
        Cause::Request(RequestCause::Malformed {
            detail: DiagnosticText::support_only(detail.to_owned()),
        }),
    )
    .protocol(Protocol::Imap)
    .try_build()
    .expect("valid account error classification")
}

/// Build a `SyncState(SchemaIncompatible)` `AccountError` for IMAP
/// cursor envelope decoding failures. The cursor bytes are structurally
/// unrecognizable, which triggers the `SchemaIncompatible` engine
/// directive to clear cursor state and re-establish from inventory.
fn schema_incompatible(detail: &str) -> AccountError {
    use bifrost_types::{AccountOperation, StateCause};
    AccountErrorBuilder::new(
        AccountErrorKind::SyncState(SyncStateErrorKind::SchemaIncompatible),
        Cause::State(StateCause::SchemaIncompatible),
    )
    .protocol(Protocol::Imap)
    .operation(AccountOperation::EstablishCursor)
    .text(DiagnosticText::support_only(detail.to_owned()))
    .try_build()
    .expect("valid account error classification")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cursor_envelope_roundtrips_all_tiers() {
        let cursors = [
            FolderCursor::QResync {
                uidvalidity: 7,
                modseq: 99,
                known_uids: CompactUidSet::from_uids([1, 2]),
                known_uids_complete: true,
            },
            FolderCursor::Condstore {
                uidvalidity: 7,
                modseq: 99,
                known_uids: CompactUidSet::from_uids([1, 3, 4]),
            },
            FolderCursor::Basic {
                uidvalidity: 7,
                uidnext: 20,
                known_uids: CompactUidSet::from_uids([1, 2, 10]),
            },
        ];

        for cursor in cursors {
            let change = encode_cursor(CursorScope::Account, &cursor);
            assert_eq!(decode_cursor(&change).expect("decode"), cursor);
        }
    }

    #[test]
    fn cursor_envelope_rejects_version_drift() {
        let mut change = encode_cursor(
            CursorScope::Account,
            &FolderCursor::QResync {
                uidvalidity: 1,
                modseq: 2,
                known_uids: CompactUidSet::default(),
                known_uids_complete: true,
            },
        );
        change.server_state.envelope_version = ENVELOPE_VERSION + 1;
        let err = decode_cursor(&change).expect_err("should fail on version mismatch");
        assert!(
            matches!(
                err.kind(),
                AccountErrorKind::SyncState(bifrost_types::SyncStateErrorKind::SchemaIncompatible)
            ),
            "unexpected kind: {:?}",
            err.kind()
        );
    }

    #[test]
    fn qresync_cursor_decodes_legacy_payload_without_known_uids() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(MAGIC);
        bytes.push(1);
        push_u32(&mut bytes, 7);
        push_u64(&mut bytes, 99);

        let cursor = decode_folder_cursor(&bytes).expect("decode legacy qresync");
        assert_eq!(
            cursor,
            FolderCursor::QResync {
                uidvalidity: 7,
                modseq: 99,
                known_uids: CompactUidSet::default(),
                known_uids_complete: false,
            }
        );
    }

    #[test]
    fn qresync_cursor_decodes_mid_pr_payload_without_complete_flag() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(MAGIC);
        bytes.push(1);
        push_u32(&mut bytes, 7);
        push_u64(&mut bytes, 99);
        encode_uid_set(&mut bytes, &CompactUidSet::from_uids([1, 2]));

        let cursor = decode_folder_cursor(&bytes).expect("decode mid-pr qresync");
        assert_eq!(
            cursor,
            FolderCursor::QResync {
                uidvalidity: 7,
                modseq: 99,
                known_uids: CompactUidSet::from_uids([1, 2]),
                known_uids_complete: true,
            }
        );
    }

    #[test]
    fn qresync_cursor_roundtrips_incomplete_baseline() {
        let cursor = FolderCursor::QResync {
            uidvalidity: 7,
            modseq: 99,
            known_uids: CompactUidSet::default(),
            known_uids_complete: false,
        };
        let change = encode_cursor(CursorScope::Account, &cursor);
        assert_eq!(decode_cursor(&change).expect("decode"), cursor);
    }

    #[test]
    fn cursor_uid_set_rejects_backwards_range() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(MAGIC);
        bytes.push(3); // Basic
        push_u32(&mut bytes, 7); // uidvalidity
        push_u32(&mut bytes, 20); // uidnext
        push_u32(&mut bytes, 1); // one range
        push_u32(&mut bytes, 10); // start
        push_u32(&mut bytes, 5); // end < start, end != 0 -> malformed
        let err = decode_folder_cursor(&bytes).expect_err("backwards range must be rejected");
        assert!(matches!(
            err.kind(),
            AccountErrorKind::SyncState(bifrost_types::SyncStateErrorKind::SchemaIncompatible)
        ));
    }

    #[test]
    fn object_and_blob_ids_tolerate_colons_in_folder_names() {
        let folder = MailboxName::new("Work:Clients").expect("valid folder");
        let id = encode_object_id(&folder, 10, 42);
        let decoded = decode_object_id(&id).expect("object id");
        assert_eq!(decoded.folder, folder);
        assert_eq!(decoded.uidvalidity, 10);
        assert_eq!(decoded.uid, 42);

        let blob = encode_blob_id(&folder, 10, 42, Some("2.1"));
        let decoded = decode_blob_id(&blob).expect("blob id");
        assert_eq!(decoded.folder, folder);
        assert_eq!(decoded.section.as_deref(), Some("2.1"));
    }

    #[test]
    fn thread_id_tolerates_colons_in_folder_names() {
        let folder = MailboxName::new("Work:Clients").expect("valid folder");
        let id = encode_thread_id(&folder, 10, &[3, 1, 3]);
        let decoded = decode_thread_id(&id).expect("thread id");
        assert_eq!(decoded.folder, folder);
        assert_eq!(decoded.uidvalidity, 10);
        assert_eq!(decoded.uids, vec![1, 3]);
    }
}
