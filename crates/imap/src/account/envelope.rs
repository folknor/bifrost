use bifrost_types::{
    BlobId, ChangeCursor, CursorScope, Error as AccountError, ObjectId, OpaqueChangeState,
    ProtocolKind,
};

use crate::types::MailboxName;

use super::CompactUidSet;

pub(crate) const ENVELOPE_VERSION: u32 = 1;
const MAGIC: &[u8; 8] = b"IMAPCUR1";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum FolderCursor {
    QResync {
        uidvalidity: u32,
        modseq: u64,
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
    pub(crate) fn known_uids(&self) -> Option<&CompactUidSet> {
        match self {
            Self::Condstore { known_uids, .. } | Self::Basic { known_uids, .. } => Some(known_uids),
            Self::QResync { .. } => None,
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
        return Err(AccountError::CursorProtocolMismatch);
    }
    if cursor.server_state.envelope_version != ENVELOPE_VERSION {
        return Err(AccountError::CursorEnvelopeUnknown);
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
        } => {
            out.push(1);
            push_u32(&mut out, *uidvalidity);
            push_u64(&mut out, *modseq);
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
        1 => Ok(FolderCursor::QResync {
            uidvalidity: input.take_u32()?,
            modseq: input.take_u64()?,
        }),
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
        _ => Err(AccountError::SchemaIncompatible),
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
            Err(AccountError::SchemaIncompatible)
        }
    }

    fn take(&mut self, count: usize) -> Result<&'a [u8], AccountError> {
        let end = self
            .offset
            .checked_add(count)
            .ok_or(AccountError::SchemaIncompatible)?;
        let out = self
            .bytes
            .get(self.offset..end)
            .ok_or(AccountError::SchemaIncompatible)?;
        self.offset = end;
        Ok(out)
    }

    fn take_u8(&mut self) -> Result<u8, AccountError> {
        self.take(1).map(|b| b[0])
    }

    fn take_u32(&mut self) -> Result<u32, AccountError> {
        let bytes: [u8; 4] = self
            .take(4)?
            .try_into()
            .map_err(|_| AccountError::SchemaIncompatible)?;
        Ok(u32::from_le_bytes(bytes))
    }

    fn take_u64(&mut self) -> Result<u64, AccountError> {
        let bytes: [u8; 8] = self
            .take(8)?
            .try_into()
            .map_err(|_| AccountError::SchemaIncompatible)?;
        Ok(u64::from_le_bytes(bytes))
    }

    fn take_uid_set(&mut self) -> Result<CompactUidSet, AccountError> {
        let count = self.take_u32()? as usize;
        let mut ranges = Vec::with_capacity(count);
        for _ in 0..count {
            let start = self.take_u32()?;
            let end = self.take_u32()?;
            let range = if end == 0 {
                crate::types::UidRange::single(start)
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
        return Err(AccountError::Other("invalid IMAP object id".into()));
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

fn decode_len_prefixed(prefix: &str, value: &str) -> Result<(MailboxName, String), AccountError> {
    let value = value
        .strip_prefix(prefix)
        .and_then(|v| v.strip_prefix(':'))
        .ok_or_else(|| AccountError::Other("invalid IMAP id prefix".into()))?;
    let Some((len, rest)) = value.split_once(':') else {
        return Err(AccountError::Other("invalid IMAP id length".into()));
    };
    let len = len
        .parse::<usize>()
        .map_err(|_| AccountError::Other("invalid IMAP id length".into()))?;
    let folder = rest
        .get(..len)
        .ok_or_else(|| AccountError::Other("invalid IMAP id folder length".into()))?;
    let after = rest
        .get(len..)
        .and_then(|v| v.strip_prefix(':'))
        .ok_or_else(|| AccountError::Other("invalid IMAP id separator".into()))?;
    let folder =
        MailboxName::new(folder.to_owned()).map_err(|e| AccountError::Other(e.to_string()))?;
    Ok((folder, after.to_owned()))
}

fn parse_u32(value: Option<&str>) -> Result<u32, AccountError> {
    value
        .ok_or_else(|| AccountError::Other("missing IMAP id field".into()))?
        .parse::<u32>()
        .map_err(|_| AccountError::Other("invalid IMAP id integer".into()))
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
            },
        );
        change.server_state.envelope_version = ENVELOPE_VERSION + 1;
        assert!(matches!(
            decode_cursor(&change),
            Err(AccountError::CursorEnvelopeUnknown)
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
}
