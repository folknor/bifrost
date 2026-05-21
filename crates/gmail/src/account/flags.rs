use std::collections::{HashMap, HashSet};

use bifrost_types::FlagOp;

use crate::types::GmailLabel;

const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

const LABEL_UNREAD: &str = "UNREAD";
const LABEL_STARRED: &str = "STARRED";
const LABEL_DRAFT: &str = "DRAFT";
const LABEL_IMPORTANT: &str = "IMPORTANT";

const FLAG_SEEN: &str = "\\Seen";
const FLAG_FLAGGED: &str = "\\Flagged";
const FLAG_DRAFT: &str = "\\Draft";
const FLAG_IMPORTANT: &str = "$Important";

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct LabelPatch {
    pub add_label_ids: Vec<String>,
    pub remove_label_ids: Vec<String>,
    pub unsupported_flags: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CanonicalFlags {
    pub flags: Vec<String>,
    pub hash: u64,
}

pub(crate) fn canonical_flags(label_ids: &[String], labels: &[GmailLabel]) -> CanonicalFlags {
    let names_by_id = labels
        .iter()
        .map(|label| (label.id.as_str(), label.name.as_str()))
        .collect::<HashMap<_, _>>();
    let mut flags = Vec::new();

    if !label_ids.iter().any(|label| label == LABEL_UNREAD) {
        flags.push(FLAG_SEEN.to_string());
    }

    for label_id in label_ids {
        match label_id.as_str() {
            LABEL_UNREAD | "INBOX" | "SENT" | "TRASH" | "SPAM" | "CHAT" => {}
            LABEL_STARRED => flags.push(FLAG_FLAGGED.to_string()),
            LABEL_DRAFT => flags.push(FLAG_DRAFT.to_string()),
            LABEL_IMPORTANT => flags.push(FLAG_IMPORTANT.to_string()),
            other => {
                let name = names_by_id.get(other).copied().unwrap_or(other);
                flags.push(format!("$gmail-label:{other}:{name}"));
            }
        }
    }

    flags.sort();
    flags.dedup();
    let hash = fnv1a_hash(&flags);
    CanonicalFlags { flags, hash }
}

pub(crate) fn flag_set(label_ids: &[String], labels: &[GmailLabel]) -> HashSet<String> {
    canonical_flags(label_ids, labels)
        .flags
        .into_iter()
        .collect()
}

pub(crate) fn translate_flag_op(op: &FlagOp, labels: &[GmailLabel]) -> LabelPatch {
    match op {
        FlagOp::Add(flags) => patch_from_sets(flags, &HashSet::new(), labels),
        FlagOp::Remove(flags) => patch_from_sets(&HashSet::new(), flags, labels),
        FlagOp::Patch { add, remove } => patch_from_sets(add, remove, labels),
        FlagOp::Set(flags) => patch_for_set(flags, labels),
        _ => LabelPatch {
            unsupported_flags: vec!["unknown future FlagOp variant".to_string()],
            ..LabelPatch::default()
        },
    }
}

fn patch_for_set(flags: &HashSet<String>, labels: &[GmailLabel]) -> LabelPatch {
    let mut add = HashSet::new();
    let mut remove = HashSet::new();

    if contains_flag(flags, FLAG_SEEN) {
        add.insert(FLAG_SEEN.to_string());
    } else {
        remove.insert(FLAG_SEEN.to_string());
    }

    for flag in [FLAG_FLAGGED, FLAG_DRAFT, FLAG_IMPORTANT] {
        if contains_flag(flags, flag) {
            add.insert(flag.to_string());
        } else {
            remove.insert(flag.to_string());
        }
    }

    for flag in flags {
        if !is_known_flag(flag, labels) {
            add.insert(flag.clone());
        }
    }

    patch_from_sets(&add, &remove, labels)
}

fn patch_from_sets(
    add_flags: &HashSet<String>,
    remove_flags: &HashSet<String>,
    labels: &[GmailLabel],
) -> LabelPatch {
    let mut patch = LabelPatch::default();
    for flag in add_flags {
        match flag_to_add_label(flag, labels) {
            FlagTranslation::Add(label) => push_unique(&mut patch.add_label_ids, label),
            FlagTranslation::Remove(label) => push_unique(&mut patch.remove_label_ids, label),
            FlagTranslation::Unsupported(flag) => push_unique(&mut patch.unsupported_flags, flag),
        }
    }
    for flag in remove_flags {
        match flag_to_remove_label(flag, labels) {
            FlagTranslation::Add(label) => push_unique(&mut patch.add_label_ids, label),
            FlagTranslation::Remove(label) => push_unique(&mut patch.remove_label_ids, label),
            FlagTranslation::Unsupported(flag) => push_unique(&mut patch.unsupported_flags, flag),
        }
    }
    patch.add_label_ids.sort();
    patch.remove_label_ids.sort();
    patch.unsupported_flags.sort();
    patch
}

enum FlagTranslation {
    Add(String),
    Remove(String),
    Unsupported(String),
}

fn flag_to_add_label(flag: &str, labels: &[GmailLabel]) -> FlagTranslation {
    if eq_flag(flag, FLAG_SEEN) {
        FlagTranslation::Remove(LABEL_UNREAD.to_string())
    } else if eq_flag(flag, FLAG_FLAGGED) {
        FlagTranslation::Add(LABEL_STARRED.to_string())
    } else if eq_flag(flag, FLAG_DRAFT) {
        FlagTranslation::Add(LABEL_DRAFT.to_string())
    } else if eq_flag(flag, FLAG_IMPORTANT) {
        FlagTranslation::Add(LABEL_IMPORTANT.to_string())
    } else if let Some(label_id) = user_label_id_from_flag(flag, labels) {
        FlagTranslation::Add(label_id)
    } else {
        FlagTranslation::Unsupported(flag.to_string())
    }
}

fn flag_to_remove_label(flag: &str, labels: &[GmailLabel]) -> FlagTranslation {
    if eq_flag(flag, FLAG_SEEN) {
        FlagTranslation::Add(LABEL_UNREAD.to_string())
    } else if eq_flag(flag, FLAG_FLAGGED) {
        FlagTranslation::Remove(LABEL_STARRED.to_string())
    } else if eq_flag(flag, FLAG_DRAFT) {
        FlagTranslation::Remove(LABEL_DRAFT.to_string())
    } else if eq_flag(flag, FLAG_IMPORTANT) {
        FlagTranslation::Remove(LABEL_IMPORTANT.to_string())
    } else if let Some(label_id) = user_label_id_from_flag(flag, labels) {
        FlagTranslation::Remove(label_id)
    } else {
        FlagTranslation::Unsupported(flag.to_string())
    }
}

fn user_label_id_from_flag(flag: &str, labels: &[GmailLabel]) -> Option<String> {
    labels
        .iter()
        .find(|label| flag == format!("$gmail-label:{}:{}", label.id, label.name))
        .map(|label| label.id.clone())
}

fn contains_flag(flags: &HashSet<String>, needle: &str) -> bool {
    flags.iter().any(|flag| eq_flag(flag, needle))
}

fn is_known_flag(flag: &str, labels: &[GmailLabel]) -> bool {
    eq_flag(flag, FLAG_SEEN)
        || eq_flag(flag, FLAG_FLAGGED)
        || eq_flag(flag, FLAG_DRAFT)
        || eq_flag(flag, FLAG_IMPORTANT)
        || user_label_id_from_flag(flag, labels).is_some()
}

fn eq_flag(left: &str, right: &str) -> bool {
    left.eq_ignore_ascii_case(right)
}

fn push_unique(items: &mut Vec<String>, item: String) {
    if !items.contains(&item) {
        items.push(item);
    }
}

fn fnv1a_hash(flags: &[String]) -> u64 {
    let mut hash = FNV_OFFSET;
    for flag in flags {
        for byte in flag.as_bytes() {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(FNV_PRIME);
        }
        hash ^= 0xff;
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

#[cfg(test)]
mod tests {
    use super::*;

    fn set(items: &[&str]) -> HashSet<String> {
        items.iter().map(|item| (*item).to_string()).collect()
    }

    #[test]
    fn canonicalizes_gmail_labels_to_engine_flags() {
        let labels = vec![GmailLabel {
            id: "Label_1".to_string(),
            name: "Project".to_string(),
            label_type: Some("user".to_string()),
            message_list_visibility: None,
            label_list_visibility: None,
            messages_total: None,
            messages_unread: None,
            threads_total: None,
            threads_unread: None,
            color: None,
        }];
        let canonical = canonical_flags(
            &[
                "INBOX".to_string(),
                LABEL_STARRED.to_string(),
                LABEL_IMPORTANT.to_string(),
                "Label_1".to_string(),
            ],
            &labels,
        );
        assert_eq!(
            canonical.flags,
            vec![
                "$Important".to_string(),
                "$gmail-label:Label_1:Project".to_string(),
                "\\Flagged".to_string(),
                "\\Seen".to_string(),
            ]
        );
        let again = canonical_flags(
            &[
                "INBOX".to_string(),
                LABEL_STARRED.to_string(),
                LABEL_IMPORTANT.to_string(),
                "Label_1".to_string(),
            ],
            &labels,
        );
        assert_eq!(canonical.hash, again.hash);
    }

    #[test]
    fn unread_label_removes_seen_flag() {
        let canonical = canonical_flags(&[LABEL_UNREAD.to_string()], &[]);
        assert!(!canonical.flags.contains(&FLAG_SEEN.to_string()));
    }

    #[test]
    fn translates_seen_polarity_back_to_gmail_unread() {
        let patch = translate_flag_op(&FlagOp::Add(set(&[FLAG_SEEN, FLAG_FLAGGED])), &[]);
        assert_eq!(patch.add_label_ids, vec![LABEL_STARRED.to_string()]);
        assert_eq!(patch.remove_label_ids, vec![LABEL_UNREAD.to_string()]);
        assert!(patch.unsupported_flags.is_empty());
    }

    #[test]
    fn remove_seen_adds_unread() {
        let patch = translate_flag_op(&FlagOp::Remove(set(&[FLAG_SEEN])), &[]);
        assert_eq!(patch.add_label_ids, vec![LABEL_UNREAD.to_string()]);
        assert!(patch.remove_label_ids.is_empty());
    }

    #[test]
    fn set_seen_removes_unread() {
        let patch = translate_flag_op(&FlagOp::Set(set(&[FLAG_SEEN])), &[]);
        assert!(patch.add_label_ids.is_empty());
        assert_eq!(
            patch.remove_label_ids,
            vec![
                LABEL_DRAFT.to_string(),
                LABEL_IMPORTANT.to_string(),
                LABEL_STARRED.to_string(),
                LABEL_UNREAD.to_string()
            ]
        );
    }

    #[test]
    fn set_without_seen_adds_unread() {
        let patch = translate_flag_op(&FlagOp::Set(set(&[FLAG_FLAGGED])), &[]);
        assert_eq!(
            patch.add_label_ids,
            vec![LABEL_STARRED.to_string(), LABEL_UNREAD.to_string()]
        );
        assert_eq!(
            patch.remove_label_ids,
            vec![LABEL_DRAFT.to_string(), LABEL_IMPORTANT.to_string()]
        );
    }
}
