use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Instant;

use crate::types::{MailboxAttribute, MailboxInfo, MailboxName, UidRange};

use super::FolderCursor;

/// Compact sorted UID set used in Basic and CONDSTORE cursor payloads.
// protocol-specific: IMAP cursors need a compact UID baseline, not a wire sequence set.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct CompactUidSet {
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
    pub(crate) selectable: bool,
    pub(crate) delimiter: Option<char>,
    pub(crate) attributes: Vec<MailboxAttribute>,
    /// `Some(owner)` for a shared/other-user folder discovered under a
    /// non-personal namespace; `None` for the account's own personal
    /// folders. Drives `MembershipScope::Mailbox` tagging and
    /// `ErrorScope::Mailbox` scoping on revocation.
    pub(crate) shared_owner: Option<bifrost_types::MailboxId>,
    cursor: RwLock<Option<FolderCursor>>,
    modseq_by_uid: RwLock<ModSeqCache>,
    last_seen: Mutex<Option<Instant>>,
}

#[derive(Debug, Default)]
struct ModSeqCache {
    uidvalidity: Option<u32>,
    by_uid: HashMap<u32, u64>,
}

impl FolderEntry {
    pub(crate) fn from_mailbox(info: MailboxInfo) -> Self {
        Self::from_mailbox_with_owner(info, None)
    }

    pub(crate) fn from_mailbox_with_owner(
        info: MailboxInfo,
        shared_owner: Option<bifrost_types::MailboxId>,
    ) -> Self {
        let selectable = !info.attributes.iter().any(|attr| {
            matches!(
                attr,
                MailboxAttribute::NoSelect | MailboxAttribute::NonExistent
            )
        });
        Self {
            name: info.name,
            selectable,
            delimiter: info.delimiter,
            attributes: info.attributes,
            shared_owner,
            cursor: RwLock::new(None),
            modseq_by_uid: RwLock::new(ModSeqCache::default()),
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

    pub(crate) fn modseq(&self, uidvalidity: u32, uid: u32) -> Option<u64> {
        let cache = self
            .modseq_by_uid
            .read()
            .expect("folder modseq lock poisoned");
        if cache.uidvalidity != Some(uidvalidity) {
            return None;
        }
        cache.by_uid.get(&uid).copied()
    }

    pub(crate) fn record_modseq(
        &self,
        uidvalidity: u32,
        uid: u32,
        modseq: u64,
    ) -> Result<(), crate::Error> {
        if uidvalidity == 0 {
            return Err(crate::Error::Protocol(
                "MODSEQ cache update missing UIDVALIDITY".into(),
            ));
        }
        if uid == 0 {
            return Err(crate::Error::Protocol(
                "MODSEQ cache update missing UID".into(),
            ));
        }
        if modseq == 0 {
            return Err(crate::Error::Protocol("FETCH returned MODSEQ 0".into()));
        }
        let mut modseqs = self
            .modseq_by_uid
            .write()
            .map_err(|_| crate::Error::Internal("folder modseq lock poisoned".into()))?;
        if modseqs.uidvalidity != Some(uidvalidity) {
            modseqs.uidvalidity = Some(uidvalidity);
            modseqs.by_uid.clear();
        }
        modseqs.by_uid.insert(uid, modseq);
        Ok(())
    }

    pub(crate) fn clear_modseqs(&self, uidvalidity: u32, uids: &[u32]) {
        let mut modseqs = self
            .modseq_by_uid
            .write()
            .expect("folder modseq lock poisoned");
        if modseqs.uidvalidity != Some(uidvalidity) {
            return;
        }
        for uid in uids {
            modseqs.by_uid.remove(uid);
        }
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

    /// Build the registry from the personal-root LIST plus the
    /// shared/other-user folders discovered under non-personal namespaces.
    /// Personal entries carry `shared_owner: None`; each shared entry
    /// carries `Some(owner)` so membership tagging and scoped revocation
    /// can route on the owning mailbox.
    pub(crate) fn from_lists(
        personal: Vec<MailboxInfo>,
        shared: Vec<(MailboxInfo, bifrost_types::MailboxId)>,
    ) -> Self {
        let registry = Self::default();
        registry.replace_all(personal);
        registry.ingest_shared(shared);
        registry
    }

    /// Install shared/other-user folders, each tagged with its owning
    /// mailbox. Additive: existing personal entries are left in place.
    pub(crate) fn ingest_shared(&self, shared: Vec<(MailboxInfo, bifrost_types::MailboxId)>) {
        let mut map = self.by_name.write().expect("folder registry lock poisoned");
        for (info, owner) in shared {
            let entry = Arc::new(FolderEntry::from_mailbox_with_owner(info, Some(owner)));
            map.insert(entry.name.as_str().to_owned(), entry);
        }
    }

    pub(crate) fn replace_all(&self, folders: Vec<MailboxInfo>) {
        let mut map = self.by_name.write().expect("folder registry lock poisoned");
        map.clear();
        for info in folders {
            let entry = Arc::new(FolderEntry::from_mailbox(info));
            map.insert(entry.name.as_str().to_owned(), entry);
        }
    }

    pub(crate) fn apply_mailbox_event(&self, info: MailboxInfo) {
        let name = info.name.as_str().to_owned();
        let old_name = info.old_name.as_ref().map(|name| name.as_str().to_owned());
        let deleted = info
            .attributes
            .iter()
            .any(|attr| matches!(attr, MailboxAttribute::NonExistent));
        let mut map = self.by_name.write().expect("folder registry lock poisoned");
        if let Some(old_name) = old_name {
            map.remove(&old_name);
        }
        if deleted {
            map.remove(&name);
            return;
        }
        if !map.contains_key(&name) || info.old_name.is_some() {
            let entry = Arc::new(FolderEntry::from_mailbox(info));
            map.insert(name, entry);
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

    pub(crate) fn modseq(&self, folder: &MailboxName, uidvalidity: u32, uid: u32) -> Option<u64> {
        self.get(folder)
            .and_then(|entry| entry.modseq(uidvalidity, uid))
    }

    pub(crate) fn record_modseq(
        &self,
        folder: &MailboxName,
        uidvalidity: u32,
        uid: u32,
        modseq: u64,
    ) -> Result<(), crate::Error> {
        if let Some(entry) = self.get(folder) {
            return entry.record_modseq(uidvalidity, uid, modseq);
        }
        Ok(())
    }

    pub(crate) fn clear_modseqs(&self, folder: &MailboxName, uidvalidity: u32, uids: &[u32]) {
        if let Some(entry) = self.get(folder) {
            entry.clear_modseqs(uidvalidity, uids);
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
    }

    #[test]
    fn folder_entry_tracks_modseq_by_uid() {
        let info = MailboxInfo {
            name: MailboxName::new("INBOX").expect("valid mailbox"),
            ..Default::default()
        };
        let entry = FolderEntry::from_mailbox(info);

        entry.record_modseq(11, 7, 99).expect("valid modseq");
        assert!(entry.record_modseq(0, 7, 100).is_err());
        assert!(entry.record_modseq(11, 0, 100).is_err());
        assert!(entry.record_modseq(11, 8, 0).is_err());

        assert_eq!(entry.modseq(11, 7), Some(99));
        assert_eq!(entry.modseq(0, 7), None);
        assert_eq!(entry.modseq(11, 0), None);
        assert_eq!(entry.modseq(11, 8), None);
        assert_eq!(entry.modseq(12, 7), None);

        entry.clear_modseqs(11, &[7]);
        assert_eq!(entry.modseq(11, 7), None);
    }

    #[test]
    fn folder_entry_drops_modseqs_from_old_uidvalidity_epoch() {
        let info = MailboxInfo {
            name: MailboxName::new("INBOX").expect("valid mailbox"),
            ..Default::default()
        };
        let entry = FolderEntry::from_mailbox(info);

        entry.record_modseq(11, 7, 99).expect("valid modseq");
        entry.record_modseq(12, 7, 100).expect("valid modseq");

        assert_eq!(entry.modseq(11, 7), None);
        assert_eq!(entry.modseq(12, 7), Some(100));
    }

    #[test]
    fn mailbox_delete_and_recreate_gets_fresh_entry() {
        let folder = MailboxName::new("Projects").expect("valid mailbox");
        let registry = FolderRegistry::from_list(vec![MailboxInfo {
            name: folder.clone(),
            ..Default::default()
        }]);
        let entry = registry.get(&folder).expect("folder entry");
        entry.record_modseq(11, 7, 99).expect("valid modseq");

        registry.apply_mailbox_event(MailboxInfo {
            name: folder.clone(),
            attributes: vec![MailboxAttribute::NonExistent],
            ..Default::default()
        });
        assert!(registry.get(&folder).is_none());

        registry.apply_mailbox_event(MailboxInfo {
            name: folder.clone(),
            ..Default::default()
        });
        let entry = registry.get(&folder).expect("recreated folder entry");
        assert_eq!(entry.modseq(11, 7), None);
        assert!(entry.cursor().is_none());
    }

    #[test]
    fn mailbox_rename_removes_old_entry() {
        let old = MailboxName::new("Old").expect("valid mailbox");
        let new = MailboxName::new("New").expect("valid mailbox");
        let registry = FolderRegistry::from_list(vec![MailboxInfo {
            name: old.clone(),
            ..Default::default()
        }]);

        registry.apply_mailbox_event(MailboxInfo {
            name: new.clone(),
            old_name: Some(old.clone()),
            ..Default::default()
        });

        assert!(registry.get(&old).is_none());
        assert!(registry.get(&new).is_some());
    }
}
