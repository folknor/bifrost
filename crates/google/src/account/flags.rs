use std::collections::{HashMap, HashSet};

use bifrost_types::FlagOp;

use crate::types::GmailLabel;

const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

const LABEL_UNREAD: &str = "UNREAD";
const LABEL_STARRED: &str = "STARRED";
const LABEL_DRAFT: &str = "DRAFT";
const LABEL_IMPORTANT: &str = "IMPORTANT";

/// Gmail's mutually exclusive display containers.
///
/// Gmail shows a message in whichever of these it carries, whatever
/// else is also attached: a spam message filed into a user label is
/// still rendered under Spam. So a relocation has to strip every one
/// of them the message is *not* moving into; adding the destination
/// alone is a no-op from the user's point of view.
pub(crate) const LABEL_INBOX: &str = "INBOX";
pub(crate) const LABEL_SPAM: &str = "SPAM";
pub(crate) const LABEL_TRASH: &str = "TRASH";
const EXCLUSIVE_CONTAINERS: [&str; 3] = [LABEL_INBOX, LABEL_SPAM, LABEL_TRASH];

/// Synthetic bifrost container id for Gmail archive.
///
/// Gmail models archive as the *absence* of every exclusive container
/// rather than as a label, so `containers_list` synthesises this id
/// purely to give the role table an `Archive` entry. It is not a real
/// Gmail label id and must never reach `addLabelIds` - Gmail rejects
/// the modify if it does.
pub(crate) const ARCHIVE_ID: &str = "archive";

pub(crate) fn is_archive_id(id: &str) -> bool {
    id.eq_ignore_ascii_case(ARCHIVE_ID)
}

/// The single Gmail relocation rule, shared by the bulk driver and the
/// single-object builders so a consumer cannot get different wire
/// semantics depending on which entry point it reached.
///
/// "Move into `destination`" lowers to: add `destination` (unless it is
/// the synthetic archive, which has no label to add), and remove every
/// exclusive display container that is not the destination itself.
///
/// - `INBOX` -> add INBOX, drop SPAM + TRASH (un-spam / un-trash).
/// - `archive` -> add nothing, drop INBOX + SPAM + TRASH.
/// - a user label -> add it, drop INBOX + SPAM + TRASH, so filing a
///   spammed or trashed message actually takes it out of Spam / Trash.
/// - `SPAM` / `TRASH` -> add it, drop the other two.
pub(crate) fn move_placement_patch(destination: &str) -> LabelPatch {
    let add_label_ids = if is_archive_id(destination) {
        Vec::new()
    } else {
        vec![destination.to_string()]
    };
    let remove_label_ids = EXCLUSIVE_CONTAINERS
        .iter()
        .filter(|container| !destination.eq_ignore_ascii_case(container))
        .map(|container| (*container).to_string())
        .collect();
    LabelPatch {
        add_label_ids,
        remove_label_ids,
        unsupported_flags: Vec::new(),
    }
}

const FLAG_SEEN: &str = "\\Seen";
const FLAG_FLAGGED: &str = "\\Flagged";
const FLAG_DRAFT: &str = "\\Draft";
const FLAG_IMPORTANT: &str = "$Important";

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct LabelPatch {
    pub(crate) add_label_ids: Vec<String>,
    pub(crate) remove_label_ids: Vec<String>,
    pub(crate) unsupported_flags: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CanonicalFlags {
    pub(crate) flags: Vec<String>,
    pub(crate) hash: u64,
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
    fn move_to_inbox_drops_the_other_exclusive_containers() {
        let patch = move_placement_patch(LABEL_INBOX);
        assert_eq!(patch.add_label_ids, vec![LABEL_INBOX.to_string()]);
        assert_eq!(
            patch.remove_label_ids,
            vec![LABEL_SPAM.to_string(), LABEL_TRASH.to_string()]
        );
    }

    #[test]
    fn move_to_user_label_drops_spam_and_trash_too() {
        let patch = move_placement_patch("Label_42");
        assert_eq!(patch.add_label_ids, vec!["Label_42".to_string()]);
        assert_eq!(
            patch.remove_label_ids,
            vec![
                LABEL_INBOX.to_string(),
                LABEL_SPAM.to_string(),
                LABEL_TRASH.to_string()
            ],
            "filing a spammed or trashed message must take it out of Spam / Trash"
        );
    }

    #[test]
    fn move_to_archive_adds_no_label() {
        let patch = move_placement_patch(ARCHIVE_ID);
        assert!(
            patch.add_label_ids.is_empty(),
            "`archive` is synthetic and is not a Gmail label id"
        );
        assert_eq!(
            patch.remove_label_ids,
            vec![
                LABEL_INBOX.to_string(),
                LABEL_SPAM.to_string(),
                LABEL_TRASH.to_string()
            ]
        );
    }

    #[test]
    fn move_to_spam_keeps_spam_and_drops_the_rest() {
        let patch = move_placement_patch(LABEL_SPAM);
        assert_eq!(patch.add_label_ids, vec![LABEL_SPAM.to_string()]);
        assert_eq!(
            patch.remove_label_ids,
            vec![LABEL_INBOX.to_string(), LABEL_TRASH.to_string()]
        );
    }

    #[test]
    fn exclusive_container_match_is_case_insensitive() {
        let patch = move_placement_patch("inbox");
        assert_eq!(patch.add_label_ids, vec!["inbox".to_string()]);
        assert_eq!(
            patch.remove_label_ids,
            vec![LABEL_SPAM.to_string(), LABEL_TRASH.to_string()],
            "a lowercased INBOX must not ask Gmail to remove the container it is moving into"
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

    fn work_label() -> Vec<GmailLabel> {
        vec![GmailLabel {
            id: "Label_1".to_string(),
            name: "Work".to_string(),
            label_type: Some("user".to_string()),
            color: None,
        }]
    }

    /// A user-label flag whose spelling matches `<id>:<name>` in the
    /// known vocabulary translates to the bare Gmail label id.
    #[test]
    fn user_label_flags_translate_to_their_gmail_label_id() {
        let patch = translate_flag_op(
            &FlagOp::Add(set(&["$gmail-label:Label_1:Work"])),
            &work_label(),
        );
        assert_eq!(patch.add_label_ids, vec!["Label_1".to_string()]);
        assert!(patch.unsupported_flags.is_empty());

        let patch = translate_flag_op(
            &FlagOp::Remove(set(&["$gmail-label:Label_1:Work"])),
            &work_label(),
        );
        assert_eq!(patch.remove_label_ids, vec!["Label_1".to_string()]);
    }

    /// DOCUMENTS A BUG, NOT AN ENDORSEMENT. The user-label lookup keys
    /// on the WHOLE `<id>:<name>` spelling, so a label renamed on the
    /// server between the read that minted the flag and the write that
    /// replays it no longer resolves. The flag lands in
    /// `unsupported_flags`, and `apply_label_patch` turns a non-empty
    /// `unsupported_flags` into `MutationSuccess::Skipped` for every id
    /// in the batch - a success lane. A stale name therefore silently
    /// drops the whole flag operation for up to 1000 messages instead
    /// of resolving by id or reporting a failure.
    #[test]
    fn a_user_label_flag_with_a_stale_name_becomes_unsupported() {
        let patch = translate_flag_op(
            &FlagOp::Add(set(&["$gmail-label:Label_1:OldName"])),
            &work_label(),
        );
        assert!(patch.add_label_ids.is_empty());
        assert_eq!(
            patch.unsupported_flags,
            vec!["$gmail-label:Label_1:OldName".to_string()],
            "the id is right there in the flag, but the lookup requires the name to match too"
        );
    }

    /// Same shape, reached the other way: an empty label vocabulary
    /// (the state a freshly opened account's scope cache is in) cannot
    /// resolve any user label at all.
    #[test]
    fn an_empty_label_vocabulary_makes_every_user_label_unsupported() {
        let patch = translate_flag_op(&FlagOp::Add(set(&["$gmail-label:Label_1:Work"])), &[]);
        assert!(patch.add_label_ids.is_empty());
        assert_eq!(patch.unsupported_flags.len(), 1);
    }

    /// DOCUMENTS A BUG, NOT AN ENDORSEMENT. `FlagOp::Set` means "the
    /// flag set is exactly this". `patch_for_set` re-derives add/remove
    /// over the four canonical flags but never walks the known user
    /// label vocabulary, so a user label the message currently carries
    /// and the `Set` omits is left attached. `reference/google.md`
    /// describes `Set` as re-deriving "over the canonical four flags
    /// plus the user label vocabulary", which the code does not do.
    #[test]
    fn set_does_not_remove_user_labels_the_new_set_omits() {
        let patch = translate_flag_op(&FlagOp::Set(set(&[FLAG_SEEN])), &work_label());
        assert!(
            !patch.remove_label_ids.contains(&"Label_1".to_string()),
            "an exact-set operation leaves known user labels attached",
        );
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

    /// An unknown flag inside a `Set` is added verbatim and then fails
    /// to translate, so the whole patch is poisoned into the
    /// unsupported lane rather than applying the parts it understood.
    #[test]
    fn an_unknown_flag_inside_a_set_poisons_the_whole_patch() {
        let patch = translate_flag_op(&FlagOp::Set(set(&[FLAG_SEEN, "$Junk"])), &[]);
        assert_eq!(patch.unsupported_flags, vec!["$Junk".to_string()]);
        assert!(
            !patch.remove_label_ids.is_empty(),
            "the canonical half still translates; the driver skips on unsupported_flags",
        );
    }

    /// Flag comparison is ASCII-case-insensitive, matching IMAP
    /// keyword semantics, so `\seen` and `\Seen` are the same flag.
    #[test]
    fn canonical_flag_matching_is_case_insensitive() {
        let patch = translate_flag_op(&FlagOp::Add(set(&["\\sEeN", "$important"])), &[]);
        assert_eq!(patch.remove_label_ids, vec![LABEL_UNREAD.to_string()]);
        assert_eq!(patch.add_label_ids, vec![LABEL_IMPORTANT.to_string()]);
        assert!(patch.unsupported_flags.is_empty());
    }

    /// Adding and removing the same flag in one `Patch` is a caller
    /// contradiction; the label lands in both lists and Gmail resolves
    /// it (removal wins). Pinned so the behaviour is a decision rather
    /// than an accident.
    #[test]
    fn a_contradictory_patch_lands_the_label_in_both_lists() {
        let patch = translate_flag_op(
            &FlagOp::Patch {
                add: set(&[FLAG_FLAGGED]),
                remove: set(&[FLAG_FLAGGED]),
            },
            &[],
        );
        assert_eq!(patch.add_label_ids, vec![LABEL_STARRED.to_string()]);
        assert_eq!(patch.remove_label_ids, vec![LABEL_STARRED.to_string()]);
    }

    #[test]
    fn empty_flag_ops_produce_an_empty_patch() {
        let patch = translate_flag_op(&FlagOp::Add(HashSet::new()), &[]);
        assert_eq!(patch, LabelPatch::default());
        let patch = translate_flag_op(&FlagOp::Remove(HashSet::new()), &[]);
        assert_eq!(patch, LabelPatch::default());
    }

    /// The canonical projection is order-independent: Gmail returns
    /// `labelIds` in no guaranteed order, so the hash must not depend
    /// on it.
    #[test]
    fn canonical_flags_are_order_independent_and_deduplicated() {
        let forward = canonical_flags(
            &[
                LABEL_STARRED.to_string(),
                LABEL_IMPORTANT.to_string(),
                "Label_1".to_string(),
            ],
            &work_label(),
        );
        let reversed = canonical_flags(
            &[
                "Label_1".to_string(),
                LABEL_IMPORTANT.to_string(),
                LABEL_STARRED.to_string(),
            ],
            &work_label(),
        );
        assert_eq!(forward.flags, reversed.flags);
        assert_eq!(forward.hash, reversed.hash);

        let duplicated =
            canonical_flags(&[LABEL_STARRED.to_string(), LABEL_STARRED.to_string()], &[]);
        assert_eq!(
            duplicated.flags,
            vec![FLAG_FLAGGED.to_string(), FLAG_SEEN.to_string()]
        );
    }

    /// Folder-like Gmail labels are containers, not flags, and must not
    /// leak into the canonical flag set.
    #[test]
    fn folder_shaped_labels_drop_out_of_the_flag_projection() {
        let canonical = canonical_flags(
            &[
                LABEL_INBOX.to_string(),
                "SENT".to_string(),
                LABEL_TRASH.to_string(),
                LABEL_SPAM.to_string(),
                "CHAT".to_string(),
            ],
            &[],
        );
        assert_eq!(
            canonical.flags,
            vec![FLAG_SEEN.to_string()],
            "only the derived \\Seen survives a folder-only label set"
        );
    }

    /// DOCUMENTS CURRENT BEHAVIOUR. An unknown label id renders with
    /// the id in the name position, so the flag spelling silently
    /// changes once the label list catches up. That is the same
    /// mechanism that makes `flags_hash` cache-order dependent.
    #[test]
    fn an_unknown_label_id_renders_its_id_in_the_name_slot() {
        let canonical = canonical_flags(&["Label_9".to_string()], &[]);
        assert!(
            canonical
                .flags
                .contains(&"$gmail-label:Label_9:Label_9".to_string())
        );
    }

    /// The `UNREAD` check that derives `\Seen` is an exact match, while
    /// the exclusive-container check in `move_placement_patch` is
    /// case-insensitive. Gmail only ever emits uppercase system ids, so
    /// this is latent, but the two halves of the crate disagree.
    #[test]
    fn the_unread_projection_is_case_sensitive_unlike_the_move_rule() {
        let upper = canonical_flags(&[LABEL_UNREAD.to_string()], &[]);
        assert!(!upper.flags.contains(&FLAG_SEEN.to_string()));

        let lower = canonical_flags(&["unread".to_string()], &[]);
        assert!(
            lower.flags.contains(&FLAG_SEEN.to_string()),
            "a lowercased UNREAD is not recognised and the message reads as seen",
        );
    }

    /// One label spelled `ab` and two labels spelled `a` and `b` must
    /// not collide. The `0xff` terminator the hash mixes after every
    /// flag is what separates them.
    #[test]
    fn one_long_label_does_not_hash_like_two_short_ones() {
        let joined = canonical_flags(&["Label_ab".to_string()], &[]);
        let split = canonical_flags(&["Label_a".to_string(), "Label_b".to_string()], &[]);
        assert_ne!(joined.hash, split.hash);
    }
}
