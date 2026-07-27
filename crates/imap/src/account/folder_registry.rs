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
    /// The MYRIGHTS rights set observed for this folder at discovery, when
    /// the server advertises ACL and answered. `None` means "not
    /// reported" (no ACL capability, MYRIGHTS failed, or a personal folder
    /// we never probed) - distinct from an explicit empty rights set.
    /// Projected onto `Container::rights` by `containers_list`; the
    /// authoritative per-operation gate is still the live `NO [ACL]`.
    pub(crate) rights: Option<crate::types::MailboxRights>,
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
        Self::from_mailbox_with_owner_rights(info, shared_owner, None)
    }

    pub(crate) fn from_mailbox_with_owner_rights(
        info: MailboxInfo,
        shared_owner: Option<bifrost_types::MailboxId>,
        rights: Option<crate::types::MailboxRights>,
    ) -> Self {
        let listed_selectable = !info.attributes.iter().any(|attr| {
            matches!(
                attr,
                MailboxAttribute::NoSelect | MailboxAttribute::NonExistent
            )
        });
        let selectable = shared_folder_is_selectable(listed_selectable, rights.as_ref());
        Self {
            name: info.name,
            selectable,
            delimiter: info.delimiter,
            attributes: info.attributes,
            shared_owner,
            rights,
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

/// One shared/other-user folder discovered under a non-personal
/// namespace, carrying its owning mailbox identity (for membership tagging
/// and scoped recovery) plus the MYRIGHTS rights set observed at
/// discovery, when the server reported one.
pub(crate) struct SharedFolderEntry {
    pub(crate) info: MailboxInfo,
    /// The owning mailbox. For an other-user namespace (root `#user/`),
    /// this is the per-principal segment that follows the root in each
    /// folder's own path - `#user/alice/INBOX` -> `MailboxId("alice")` -
    /// so distinct users get distinct owners. For a shared namespace
    /// (root `#shared.`), all folders share one owner, the root minus its
    /// trailing delimiter -> `MailboxId("#shared")`.
    pub(crate) owner: bifrost_types::MailboxId,
    /// The parsed MYRIGHTS set, or `None` when the server did not report
    /// one (no ACL capability, or MYRIGHTS failed and discovery deferred
    /// to SELECT).
    pub(crate) rights: Option<crate::types::MailboxRights>,
    /// The NAMESPACE prefix this folder was enumerated under (`#user/`,
    /// `Shared/`, ...). Carried so `ingest_shared` can tell a folder that
    /// genuinely lives in a non-personal namespace from a candidate a
    /// misbehaving server echoed out of the personal namespace.
    pub(crate) namespace_prefix: String,
}

/// Whether a shared-namespace candidate outranks an identically-named entry
/// the personal LIST already produced.
///
/// True exactly when the candidate's path actually lies under the non-empty
/// NAMESPACE prefix it was enumerated with. That prefix is the server's own
/// declaration that the path is not personal, so the overlap is the personal
/// LIST over-reporting (RFC 2342 leaves `LIST "" "*"` free to include
/// non-personal namespaces, and several servers do), not a shared probe
/// leaking personal folders.
///
/// False for an empty prefix or a path outside it, which is the case a
/// reference/pattern-ignoring server produces: those candidates must not
/// demote a genuine personal folder.
///
/// Pure so the precedence rule is unit-pinnable without a live server.
pub(crate) fn shared_overrides_personal(name: &str, namespace_prefix: &str) -> bool {
    !namespace_prefix.is_empty() && name.starts_with(namespace_prefix)
}

/// Whether a folder may be SELECTed, given LIST's own selectability
/// (`\Noselect` / `\NonExistent`) and the MYRIGHTS set discovery captured.
///
/// This is where the RFC 4314 read decision lands. Shared-folder discovery
/// used to enforce it by DROPPING an unreadable candidate from the shared
/// set, which silently demoted it: the personal `LIST "" "*"` is free to
/// echo the non-personal namespaces (RFC 2342), so the dropped path stayed
/// registered as that listing's bare entry, with no owner, no `Shared`
/// namespace, and no rights - a read-only share presenting as a writable
/// personal folder. Keeping the entry and clearing `selectable` instead
/// preserves the identity (owner, namespace, rights reach
/// `containers_list`) while still keeping the folder out of
/// `discover_cursor_scopes`, which filters on exactly this flag: no cursor
/// scope is created for a mailbox we cannot SELECT.
///
/// `None` rights means unreported (personal folder, no ACL capability, or a
/// MYRIGHTS failure that deferred to SELECT) and never gates.
///
/// Pure so the selectability decision is unit-pinnable without a server.
pub(crate) fn shared_folder_is_selectable(
    listed_selectable: bool,
    rights: Option<&crate::types::MailboxRights>,
) -> bool {
    listed_selectable && rights.is_none_or(crate::types::MailboxRights::can_read)
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
    pub(crate) fn from_lists(personal: Vec<MailboxInfo>, shared: Vec<SharedFolderEntry>) -> Self {
        let registry = Self::default();
        registry.replace_all(personal);
        registry.ingest_shared(shared);
        registry
    }

    /// Install shared/other-user folders, each tagged with its owning
    /// mailbox and MYRIGHTS set.
    ///
    /// The overlap rule is `shared_overrides_personal`: a candidate whose
    /// path lies under its own non-personal NAMESPACE prefix wins over an
    /// entry the personal `LIST "" "*"` already produced, because many
    /// servers (Dovecot among them) answer the unqualified personal LIST
    /// with the shared and other-user namespaces too. Skipping those
    /// overlaps - the prior rule - left the folder registered as a bare
    /// personal entry: no owner tag, no `Shared` namespace, and no rights,
    /// which is how a read-only shared folder became indistinguishable from
    /// a writable one downstream.
    ///
    /// A candidate NOT under its prefix is still skipped. That is the
    /// defense against a server that ignores the LIST reference/pattern
    /// split and answers a namespace LIST with personal folders: those
    /// must never demote a real personal INBOX to shared.
    pub(crate) fn ingest_shared(&self, shared: Vec<SharedFolderEntry>) {
        let mut map = self.by_name.write().expect("folder registry lock poisoned");
        for shared in shared {
            let name = shared.info.name.as_str().to_owned();
            if map.contains_key(&name)
                && !shared_overrides_personal(&name, &shared.namespace_prefix)
            {
                continue;
            }
            let entry = Arc::new(FolderEntry::from_mailbox_with_owner_rights(
                shared.info,
                Some(shared.owner),
                shared.rights,
            ));
            map.insert(name, entry);
        }
    }

    /// Replace the PERSONAL folder set, preserving the shared/other-user
    /// entries discovered under NAMESPACE at open.
    ///
    /// A mid-session re-LIST (`refresh_folders` after a create / rename /
    /// move / delete) only enumerates the personal root, so clearing the
    /// whole map would drop every shared folder along with its owner tag and
    /// MYRIGHTS - blanking the shared half of `containers_list` until the
    /// next account reopen. A personal candidate that collides with a
    /// retained shared entry does not overwrite it (the shared tagging is
    /// the more specific fact).
    pub(crate) fn replace_personal(&self, folders: Vec<MailboxInfo>) {
        let mut map = self.by_name.write().expect("folder registry lock poisoned");
        map.retain(|_, entry| entry.shared_owner.is_some());
        for info in folders {
            let name = info.name.as_str().to_owned();
            if map.contains_key(&name) {
                continue;
            }
            map.insert(name, Arc::new(FolderEntry::from_mailbox(info)));
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
        // Carry the shared-owner tag across a rename/recreate. A LIST/IDLE
        // `MailboxInfo` does not re-derive the owning mailbox (NAMESPACE
        // discovery does), so rebuilding the entry with `from_mailbox`
        // (owner `None`) would silently demote a renamed/recreated shared
        // folder to personal. A later SELECT denial on that folder would
        // then escalate account-wide (`NoPermission`) instead of routing
        // through the scoped `ScopeRevoked` quarantine. Source the owner
        // from the entry being superseded: the old name on a rename, the
        // same name on a recreate.
        // The rights set rides along with the owner tag for the same
        // reason: a LIST/IDLE `MailboxInfo` carries no MYRIGHTS, so
        // rebuilding without it would silently drop a shared folder's
        // read-only marking from `containers_list`.
        let superseded = old_name
            .as_ref()
            .and_then(|old| map.get(old))
            .or_else(|| map.get(&name));
        let inherited_owner = superseded.and_then(|entry| entry.shared_owner.clone());
        let inherited_rights = superseded.and_then(|entry| entry.rights.clone());
        if let Some(old_name) = old_name {
            map.remove(&old_name);
        }
        if deleted {
            map.remove(&name);
            return;
        }
        if !map.contains_key(&name) || info.old_name.is_some() {
            let entry = Arc::new(FolderEntry::from_mailbox_with_owner_rights(
                info,
                inherited_owner,
                inherited_rights,
            ));
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

    /// A shared-namespace discovery entry owned by `alice`, optionally
    /// carrying a MYRIGHTS wire string. Enumerated under `#user/`.
    fn shared_entry(name: &MailboxName, rights: Option<&str>) -> SharedFolderEntry {
        shared_entry_under(name, rights, "#user/")
    }

    fn shared_entry_under(
        name: &MailboxName,
        rights: Option<&str>,
        namespace_prefix: &str,
    ) -> SharedFolderEntry {
        SharedFolderEntry {
            info: MailboxInfo {
                name: name.clone(),
                ..Default::default()
            },
            owner: bifrost_types::MailboxId("alice".to_owned()),
            rights: rights.map(crate::types::MailboxRights::parse),
            namespace_prefix: namespace_prefix.to_owned(),
        }
    }

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
    fn ingest_shared_does_not_overwrite_personal_entry() {
        let shared = MailboxName::new("Shared/INBOX").expect("valid mailbox");
        let overlap = MailboxName::new("INBOX").expect("valid mailbox");
        let registry = FolderRegistry::from_lists(
            vec![MailboxInfo {
                name: overlap.clone(),
                ..Default::default()
            }],
            vec![
                shared_entry(&overlap, None),
                shared_entry(&shared, Some("lr")),
            ],
        );

        // The personal INBOX must stay personal (no owner) despite an
        // overlapping shared candidate of the same name.
        let personal = registry.get(&overlap).expect("personal entry");
        assert!(personal.shared_owner.is_none());
        // The non-overlapping shared folder is still ingested.
        let ingested = registry.get(&shared).expect("shared entry");
        assert_eq!(
            ingested.shared_owner,
            Some(bifrost_types::MailboxId("alice".to_owned()))
        );
        // The discovery-time MYRIGHTS set is retained on the entry so
        // `containers_list` can project it.
        assert_eq!(
            ingested.rights,
            Some(crate::types::MailboxRights::parse("lr"))
        );
    }

    // RFC 2342 leaves `LIST "" "*"` free to include the non-personal
    // namespaces, and several servers do. When the SAME path comes back from
    // both the personal LIST and its own namespace LIST, the namespace
    // enumeration is authoritative: skipping the overlap left the folder
    // registered bare (no owner, no rights), which is exactly how a
    // read-only shared folder became indistinguishable from a writable one.
    #[test]
    fn shared_candidate_under_its_prefix_upgrades_an_overlapping_personal_entry() {
        let path = MailboxName::new("Shared/alice/Reports").expect("valid mailbox");
        let registry = FolderRegistry::from_lists(
            // The personal LIST already returned the shared path.
            vec![MailboxInfo {
                name: path.clone(),
                ..Default::default()
            }],
            vec![shared_entry_under(&path, Some("lr"), "Shared/")],
        );

        let entry = registry.get(&path).expect("entry present");
        assert_eq!(
            entry.shared_owner,
            Some(bifrost_types::MailboxId("alice".to_owned())),
            "a path under a declared shared namespace must carry its owner"
        );
        assert_eq!(
            entry.rights,
            Some(crate::types::MailboxRights::parse("lr")),
            "the MYRIGHTS set must survive the personal-LIST overlap"
        );
    }

    // The exact downstream shape: ONE grantee shares TWO folders under ONE
    // other-users prefix, one writable and one read-only, and the personal
    // LIST echoes both (RFC 2342). Earlier tests fed disjoint lists, so the
    // asymmetry never showed: discovery dropped the unreadable candidate,
    // the personal echo stayed, and the read-only share arrived at the
    // consumer as a writable PERSONAL folder. Both must carry owner,
    // namespace and rights; only the rights may differ.
    #[test]
    fn writable_and_read_only_shares_under_one_prefix_both_keep_owner_and_rights() {
        let writable = MailboxName::new("Shared/alice/Reports").expect("valid mailbox");
        let read_only = MailboxName::new("Shared/alice/Read Only").expect("valid mailbox");
        let registry = FolderRegistry::from_lists(
            vec![
                MailboxInfo {
                    name: writable.clone(),
                    ..Default::default()
                },
                MailboxInfo {
                    name: read_only.clone(),
                    ..Default::default()
                },
            ],
            vec![
                shared_entry_under(&writable, Some("lrswipkxte"), "Shared/"),
                shared_entry_under(&read_only, Some("lr"), "Shared/"),
            ],
        );

        let alice = Some(bifrost_types::MailboxId("alice".to_owned()));
        let w = registry.get(&writable).expect("writable entry");
        let r = registry.get(&read_only).expect("read-only entry");
        assert_eq!(w.shared_owner, alice);
        assert_eq!(
            r.shared_owner, alice,
            "a read-only share must carry the same owner as its writable sibling"
        );
        assert_eq!(
            w.rights,
            Some(crate::types::MailboxRights::parse("lrswipkxte"))
        );
        assert_eq!(
            r.rights,
            Some(crate::types::MailboxRights::parse("lr")),
            "the read-only rights set must reach the registry, not be discarded"
        );
        // Both are readable, so both stay selectable: rights are the ONLY
        // difference between the two entries.
        assert!(w.selectable);
        assert!(r.selectable);
    }

    // A share the grantee cannot read keeps its shared identity (so it can
    // never be mistaken for a personal folder) but is withheld from cursor
    // scoping, which filters on `selectable`.
    #[test]
    fn unreadable_share_stays_shared_but_unselectable() {
        let path = MailboxName::new("Shared/alice/No Read").expect("valid mailbox");
        let registry = FolderRegistry::from_lists(
            vec![MailboxInfo {
                name: path.clone(),
                ..Default::default()
            }],
            vec![shared_entry_under(&path, Some("l"), "Shared/")],
        );

        let entry = registry.get(&path).expect("entry present");
        assert_eq!(
            entry.shared_owner,
            Some(bifrost_types::MailboxId("alice".to_owned()))
        );
        assert_eq!(entry.rights, Some(crate::types::MailboxRights::parse("l")));
        assert!(
            !entry.selectable,
            "an unreadable share must not become a cursor scope"
        );
    }

    #[test]
    fn selectability_gates_on_rights_only_when_rights_were_reported() {
        // Unreported rights never gate (personal folder, or no ACL).
        assert!(shared_folder_is_selectable(true, None));
        // `\Noselect` still wins regardless of rights.
        assert!(!shared_folder_is_selectable(
            false,
            Some(&crate::types::MailboxRights::parse("lrswipkxte"))
        ));
        // Read-only (`lr`) is selectable: `l`+`r` is the SELECT/FETCH pair.
        assert!(shared_folder_is_selectable(
            true,
            Some(&crate::types::MailboxRights::parse("lr"))
        ));
        // Lookup without read is visible but not selectable.
        assert!(!shared_folder_is_selectable(
            true,
            Some(&crate::types::MailboxRights::parse("l"))
        ));
    }

    #[test]
    fn shared_precedence_needs_the_path_to_be_under_the_prefix() {
        // The overlap case a namespace LIST genuinely outranks.
        assert!(shared_overrides_personal("Shared/alice/INBOX", "Shared/"));
        // A server that ignores the LIST reference/pattern split answers a
        // namespace probe with personal folders; those must never demote a
        // real personal INBOX.
        assert!(!shared_overrides_personal("INBOX", "Shared/"));
        // An empty prefix carries no claim at all.
        assert!(!shared_overrides_personal("INBOX", ""));
        // Textual prefix match is what the server itself declared, so a
        // sibling root that merely shares a leading substring still counts
        // only when the declared prefix matches.
        assert!(!shared_overrides_personal("SharedOther/x", "#user/"));
    }

    // A mid-session personal re-LIST (after a create / rename / move /
    // delete) must not take the shared entries down with it.
    #[test]
    fn replace_personal_retains_shared_entries_with_owner_and_rights() {
        let personal = MailboxName::new("INBOX").expect("valid mailbox");
        let shared = MailboxName::new("Shared/alice/Reports").expect("valid mailbox");
        let registry = FolderRegistry::from_lists(
            vec![MailboxInfo {
                name: personal.clone(),
                ..Default::default()
            }],
            vec![shared_entry_under(&shared, Some("lrswipkxte"), "Shared/")],
        );

        let created = MailboxName::new("Projects").expect("valid mailbox");
        registry.replace_personal(vec![
            MailboxInfo {
                name: personal.clone(),
                ..Default::default()
            },
            MailboxInfo {
                name: created.clone(),
                ..Default::default()
            },
        ]);

        assert!(registry.get(&created).is_some(), "new personal folder lands");
        let retained = registry.get(&shared).expect("shared entry retained");
        assert_eq!(
            retained.shared_owner,
            Some(bifrost_types::MailboxId("alice".to_owned()))
        );
        assert_eq!(
            retained.rights,
            Some(crate::types::MailboxRights::parse("lrswipkxte")),
            "a personal re-LIST must not blank shared rights"
        );
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

    #[test]
    fn rename_preserves_shared_owner_tag() {
        let old = MailboxName::new("Shared/alice/Old").expect("valid mailbox");
        let new = MailboxName::new("Shared/alice/New").expect("valid mailbox");
        let registry =
            FolderRegistry::from_lists(Vec::new(), vec![shared_entry(&old, Some("lrs"))]);

        registry.apply_mailbox_event(MailboxInfo {
            name: new.clone(),
            old_name: Some(old.clone()),
            ..Default::default()
        });

        let renamed = registry.get(&new).expect("renamed shared entry");
        assert_eq!(
            renamed.shared_owner,
            Some(bifrost_types::MailboxId("alice".to_owned())),
            "a renamed shared folder must keep its owner so a later SELECT denial quarantines",
        );
        assert_eq!(
            renamed.rights,
            Some(crate::types::MailboxRights::parse("lrs")),
            "a renamed shared folder must keep its MYRIGHTS projection",
        );
    }

    #[test]
    fn recreate_preserves_shared_owner_tag() {
        let folder = MailboxName::new("Shared/alice/Proj").expect("valid mailbox");
        let registry = FolderRegistry::from_lists(Vec::new(), vec![shared_entry(&folder, None)]);

        // Recreate at the same name (fresh UIDVALIDITY epoch): a same-name
        // event with old_name set, or a contains-key replace path.
        registry.apply_mailbox_event(MailboxInfo {
            name: folder.clone(),
            old_name: Some(folder.clone()),
            ..Default::default()
        });

        let recreated = registry.get(&folder).expect("recreated shared entry");
        assert_eq!(
            recreated.shared_owner,
            Some(bifrost_types::MailboxId("alice".to_owned())),
        );
    }
}
