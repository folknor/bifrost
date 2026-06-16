//! Foreign (shared/delegate) JMAP account mailbox codec.
//!
//! A non-personal JMAP account's mailboxes surface as
//! `CursorScope::Folder(FolderId(encode_foreign(account_id, mailbox_id)))`.
//! The owning JMAP `accountId` rides inside the `FolderId` string so the
//! scope is distinct per (account, mailbox) and self-routing on a cold
//! cursor resume. `Type(_)` scopes cannot carry a foreign account
//! (`Type(Email)` is the same value for every account, so identical
//! scopes for different accounts would collide in the engine's cursor /
//! membership index); the `Folder` shape is the variant-free resolution.
//!
//! Identical in shape to the Graph codec but per-crate: here the
//! `mailbox` field is a JMAP `accountId` and `folder` is a native JMAP
//! mailbox id, where Graph carries a `/users/{id}` routing key - so a
//! shared `bifrost-types` type would be a false generalization.

use bifrost_types::{FolderId, MailboxId, MembershipScope};

/// Reserved separator. A `FolderId` of the form
/// `"<accountId>\u{1f}<mailboxId>"` is a foreign-account mailbox. `\u{1f}`
/// (US, unit separator) cannot appear in a JMAP id (RFC 8620 §1.2 ids are
/// printable-ASCII-ish and never contain control characters), so it is an
/// unambiguous delimiter.
const FOREIGN_SEP: char = '\u{1f}';

/// A foreign-account mailbox: the owning JMAP `accountId` plus the native
/// JMAP mailbox id within it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ForeignMailbox {
    /// The foreign JMAP `accountId`.
    pub(crate) account_id: String,
    /// The native JMAP mailbox id.
    pub(crate) mailbox_id: String,
}

/// Encode a `(accountId, mailboxId)` pair into the namespaced `FolderId`
/// carried in a foreign `Folder` scope.
pub(crate) fn encode_foreign(account_id: &str, mailbox_id: &str) -> FolderId {
    FolderId(format!("{account_id}{FOREIGN_SEP}{mailbox_id}"))
}

/// Parse a foreign `FolderId` back into its `(accountId, mailboxId)`
/// pair. Returns `None` for a plain id with no separator (which never
/// occurs for JMAP, whose primary scopes are `Type(_)`, but guards the
/// boundary regardless).
pub(crate) fn parse_foreign(folder: &FolderId) -> Option<ForeignMailbox> {
    folder
        .0
        .split_once(FOREIGN_SEP)
        .map(|(account_id, mailbox_id)| ForeignMailbox {
            account_id: account_id.to_string(),
            mailbox_id: mailbox_id.to_string(),
        })
}

/// The `MembershipScope` owner tag for a foreign mailbox: the owning JMAP
/// `accountId` as a `MailboxId`. Emitted on discovery so the consumer
/// maps the scope to its shared-account owner.
pub(crate) fn owner_tag(account_id: &str) -> MembershipScope {
    MembershipScope::Mailbox(MailboxId(account_id.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn foreign_round_trips_through_folder_id() {
        let id = encode_foreign("acct-99", "mbx-12");
        let parsed = parse_foreign(&id).expect("foreign");
        assert_eq!(parsed.account_id, "acct-99");
        assert_eq!(parsed.mailbox_id, "mbx-12");
    }

    #[test]
    fn plain_id_is_not_foreign() {
        assert!(parse_foreign(&FolderId("mbx-12".to_string())).is_none());
    }

    #[test]
    fn owner_tag_is_mailbox_membership() {
        assert_eq!(
            owner_tag("acct-99"),
            MembershipScope::Mailbox(MailboxId("acct-99".to_string()))
        );
    }

    #[test]
    fn splits_on_first_separator_only() {
        let id = FolderId(format!("acct{FOREIGN_SEP}mbx{FOREIGN_SEP}weird"));
        let parsed = parse_foreign(&id).expect("foreign");
        assert_eq!(parsed.account_id, "acct");
        assert_eq!(parsed.mailbox_id, format!("mbx{FOREIGN_SEP}weird"));
    }
}
