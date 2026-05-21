use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Instant;

use crate::types::{MailboxAttribute, MailboxInfo, MailboxName, UidRange};

use super::FolderCursor;

/// Compact sorted UID set used in Basic and CONDSTORE cursor payloads.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CompactUidSet {
    ranges: Vec<UidRange>,
}

impl CompactUidSet {
    pub(crate) fn from_uids(uids: impl IntoIterator<Item = u32>) -> Self {
        let mut values: Vec<u32> = uids.into_iter().filter(|uid| *uid != 0).collect();
        values.sort_unstable();
        values.dedup();

        let mut ranges = Vec::new();
        let mut iter = values.into_iter();
        let Some(mut start) = iter.next() else {
            return Self { ranges };
        };
        let mut end = start;
        for uid in iter {
            if uid == end.saturating_add(1) {
                end = uid;
            } else {
                ranges.push(if start == end {
                    UidRange::single(start)
                } else {
                    UidRange::range(start, end)
                });
                start = uid;
                end = uid;
            }
        }
        ranges.push(if start == end {
            UidRange::single(start)
        } else {
            UidRange::range(start, end)
        });
        Self { ranges }
    }

    pub(crate) fn from_ranges(ranges: Vec<UidRange>) -> Self {
        Self::from_uids(ranges.into_iter().flat_map(expand_range))
    }

    pub(crate) fn ranges(&self) -> &[UidRange] {
        &self.ranges
    }

    pub(crate) fn to_uids(&self) -> Vec<u32> {
        self.ranges.iter().copied().flat_map(expand_range).collect()
    }

    pub(crate) fn len(&self) -> usize {
        self.ranges
            .iter()
            .map(|range| {
                range
                    .end
                    .map(|end| end.saturating_sub(range.start).saturating_add(1))
                    .unwrap_or(1)
            })
            .map(|count| usize::try_from(count).unwrap_or(usize::MAX))
            .sum()
    }

    pub(crate) fn diff(&self, newer: &Self) -> UidSetDiff {
        let old: BTreeSet<u32> = self.to_uids().into_iter().collect();
        let new: BTreeSet<u32> = newer.to_uids().into_iter().collect();
        UidSetDiff {
            added: new.difference(&old).copied().collect(),
            removed: old.difference(&new).copied().collect(),
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct UidSetDiff {
    pub(crate) added: Vec<u32>,
    pub(crate) removed: Vec<u32>,
}

pub(crate) fn expand_range(range: UidRange) -> Vec<u32> {
    match range.end {
        Some(end) => (range.start..=end).collect(),
        None => vec![range.start],
    }
}

pub(crate) struct FolderEntry {
    pub(crate) name: MailboxName,
    #[allow(dead_code)]
    pub(crate) attributes: Vec<MailboxAttribute>,
    pub(crate) selectable: bool,
    cursor: RwLock<Option<FolderCursor>>,
    last_seen: Mutex<Option<Instant>>,
}

impl FolderEntry {
    pub(crate) fn from_mailbox(info: MailboxInfo) -> Self {
        let selectable = !info.attributes.iter().any(|attr| {
            matches!(
                attr,
                MailboxAttribute::NoSelect | MailboxAttribute::NonExistent
            )
        });
        Self {
            name: info.name,
            attributes: info.attributes,
            selectable,
            cursor: RwLock::new(None),
            last_seen: Mutex::new(None),
        }
    }

    pub(crate) fn cursor(&self) -> Option<FolderCursor> {
        self.cursor
            .read()
            .expect("folder cursor lock poisoned")
            .clone()
    }

    pub(crate) fn set_cursor(&self, cursor: FolderCursor) {
        *self.cursor.write().expect("folder cursor lock poisoned") = Some(cursor);
    }

    pub(crate) fn mark_seen(&self) {
        *self
            .last_seen
            .lock()
            .expect("folder last_seen lock poisoned") = Some(Instant::now());
    }

    pub(crate) fn last_seen(&self) -> Option<Instant> {
        *self
            .last_seen
            .lock()
            .expect("folder last_seen lock poisoned")
    }
}

#[derive(Default)]
pub(crate) struct FolderRegistry {
    by_name: RwLock<HashMap<String, Arc<FolderEntry>>>,
}

impl FolderRegistry {
    pub(crate) fn from_list(folders: Vec<MailboxInfo>) -> Self {
        let registry = Self::default();
        registry.replace_all(folders);
        registry
    }

    pub(crate) fn replace_all(&self, folders: Vec<MailboxInfo>) {
        let mut map = self.by_name.write().expect("folder registry lock poisoned");
        map.clear();
        for info in folders {
            let entry = Arc::new(FolderEntry::from_mailbox(info));
            map.insert(entry.name.as_str().to_owned(), entry);
        }
    }

    pub(crate) fn entries(&self) -> Vec<Arc<FolderEntry>> {
        self.by_name
            .read()
            .expect("folder registry lock poisoned")
            .values()
            .cloned()
            .collect()
    }

    pub(crate) fn get(&self, folder: &MailboxName) -> Option<Arc<FolderEntry>> {
        self.by_name
            .read()
            .expect("folder registry lock poisoned")
            .get(folder.as_str())
            .cloned()
    }

    pub(crate) fn set_cursor(&self, folder: &MailboxName, cursor: FolderCursor) {
        if let Some(entry) = self.get(folder) {
            entry.set_cursor(cursor);
        }
    }

    pub(crate) fn mark_seen(&self, folder: &MailboxName) {
        if let Some(entry) = self.get(folder) {
            entry.mark_seen();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compact_uid_set_roundtrips_and_diffs() {
        let set = CompactUidSet::from_uids([1, 2, 3, 7, 9, 10]);
        assert_eq!(set.to_uids(), vec![1, 2, 3, 7, 9, 10]);
        assert_eq!(
            set.ranges(),
            &[
                UidRange::range(1, 3),
                UidRange::single(7),
                UidRange::range(9, 10)
            ]
        );

        let newer = CompactUidSet::from_uids([2, 3, 4, 9]);
        let diff = set.diff(&newer);
        assert_eq!(diff.added, vec![4]);
        assert_eq!(diff.removed, vec![1, 7, 10]);
        assert_eq!(set.len(), 6);
    }

    #[test]
    fn list_entry_special_use_and_selectability() {
        let info = MailboxInfo {
            name: MailboxName::new("Archive").expect("valid mailbox"),
            delimiter: Some('/'),
            attributes: vec![MailboxAttribute::Archive, MailboxAttribute::NoSelect],
            old_name: None,
            child_info: Vec::new(),
        };
        let entry = FolderEntry::from_mailbox(info);
        assert!(!entry.selectable);
        assert!(
            entry
                .attributes
                .iter()
                .any(MailboxAttribute::is_special_use)
        );
    }
}
