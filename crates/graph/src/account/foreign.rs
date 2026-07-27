//! Foreign (shared/delegate) mailbox folder codec.
//!
//! A Graph delegate/shared mailbox surfaces as ordinary
//! `CursorScope::FolderType { folder, ty }` scopes; the owning mailbox
//! identity rides inside the `FolderId` string so the scope is distinct
//! per (mailbox, folder) and self-routing on a cold cursor resume. This
//! keeps `CursorScope` untouched (the engine never inspects the payload)
//! and mirrors how IMAP namespaces other-user folders by path.
//!
//! The codec is `pub(crate)` and lives per-crate: the JMAP equivalent
//! carries an `accountId` where Graph carries a `/users/{id}` routing
//! key, so a shared `bifrost-types` type would be a false generalization.

use bifrost_types::{CursorScope, FolderId, MailboxId, MembershipScope, ObjectId};

/// Reserved separator. A `FolderId` of the form `"<mailbox>\u{1f}<folder>"`
/// is a foreign-mailbox folder; a plain id is the primary mailbox.
/// `\u{1f}` (US, unit separator) cannot appear in a Graph folder id, an
/// SMTP address, or a user id, so it is unambiguous as a delimiter.
const FOREIGN_SEP: char = '\u{1f}';

/// Reserved separator for a PUBLIC-folder item id: `"<folderId>\u{1e}<itemId>"`.
/// A distinct delimiter from `FOREIGN_SEP` because the two namespaces mean
/// different things - a foreign id carries a `/users/{mailbox}` routing key
/// and reads over Graph REST, a public id carries the EWS folder whose
/// `routing_map` entry supplies the `X-AnchorMailbox` /
/// `X-PublicFolderMailbox` pair and reads over EWS `GetItem`. `\u{1e}` (RS,
/// record separator) cannot appear in an EWS folder or item id, so the two
/// forms are mutually unambiguous.
const PUBLIC_SEP: char = '\u{1e}';

/// A foreign-mailbox folder: the routing mailbox plus the native Graph
/// folder id within it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ForeignFolder {
    /// The `/users/{mailbox}` routing key (SMTP address or user id).
    pub(crate) mailbox: String,
    /// The native Graph folder id.
    pub(crate) folder: String,
}

/// The result of parsing a `FolderId`: either a primary-mailbox folder
/// (a plain native id) or a foreign-mailbox folder.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ParsedFolder {
    Primary(String),
    Foreign(ForeignFolder),
}

impl ParsedFolder {
    /// The native Graph folder id, regardless of variant - the value that
    /// goes into a `/mailFolders/{id}` REST path.
    pub(crate) fn native_id(&self) -> &str {
        match self {
            Self::Primary(id) => id,
            Self::Foreign(foreign) => &foreign.folder,
        }
    }

    /// The foreign half, when this folder belongs to a shared mailbox.
    pub(crate) fn foreign(&self) -> Option<&ForeignFolder> {
        match self {
            Self::Primary(_) => None,
            Self::Foreign(foreign) => Some(foreign),
        }
    }
}

/// Encode a `(mailbox, native folder)` pair into the namespaced
/// `FolderId` carried in a foreign scope.
pub(crate) fn encode_foreign(mailbox: &str, folder: &str) -> FolderId {
    FolderId(format!("{mailbox}{FOREIGN_SEP}{folder}"))
}

/// Parse a `FolderId` back into a `ParsedFolder`. Splits on the first
/// `FOREIGN_SEP`; a folder id with no separator is a primary-mailbox
/// folder.
pub(crate) fn parse_folder(folder: &FolderId) -> ParsedFolder {
    match folder.0.split_once(FOREIGN_SEP) {
        Some((mailbox, native)) => ParsedFolder::Foreign(ForeignFolder {
            mailbox: mailbox.to_string(),
            folder: native.to_string(),
        }),
        None => ParsedFolder::Primary(folder.0.clone()),
    }
}

/// The `MembershipScope` owner tag for a foreign folder: the mailbox
/// identity as a `MailboxId`. Emitted on discovery so the consumer maps
/// the scope to its shared-mailbox owner. The engine's covering rule
/// cannot form this tag (the folder-id and mailbox-id strings differ).
pub(crate) fn owner_tag(mailbox: &str) -> MembershipScope {
    MembershipScope::Mailbox(MailboxId(mailbox.to_string()))
}

/// A parsed Graph message/blob `ObjectId`: either a primary-mailbox item
/// (a bare native message id, routed through `/me`) or a foreign-mailbox
/// item carrying the `/users/{mailbox}` routing key it must be fetched
/// from. The owning mailbox is encoded into the id string exactly like the
/// folder codec above (same `\u{1f}` separator, same precedent as the
/// `pim.rs` scheduled-send handle): the `bifrost-types::ObjectId` type is
/// unchanged, only its string content carries the routing key.
///
/// This is what makes hydration / blob / raw-RFC822 reads route to
/// `/users/{owner}/...` for a shared mailbox instead of 404-ing on `/me`.
/// The mint sites (inventory / changes projection) encode foreign-scope
/// ids; the request sites decode and route. A primary item is never
/// encoded, so one logical item has exactly one wire form.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ParsedMessageId {
    Primary(String),
    Foreign {
        mailbox: String,
        message: String,
    },
    /// An Exchange public-folder item. Carries the EWS folder id, because
    /// that is the `routing_map` key holding the item's
    /// `X-AnchorMailbox` / `X-PublicFolderMailbox` routing pair - a public
    /// folder has no owning mailbox of its own to route by. Read over EWS
    /// `GetItem` / `GetAttachment`, never Graph REST (`/me/messages/{id}`
    /// does not know a raw EWS `ItemId`).
    Public {
        folder: String,
        item: String,
    },
}

impl ParsedMessageId {
    /// The native Graph message id, regardless of variant - the value that
    /// goes into a `/messages/{id}` REST path or an EWS `<t:ItemId>`.
    pub(crate) fn native_id(&self) -> &str {
        match self {
            Self::Primary(id) => id,
            Self::Foreign { message, .. } => message,
            Self::Public { item, .. } => item,
        }
    }

    /// The owning mailbox routing key when this id belongs to a shared
    /// mailbox; `None` for a primary-mailbox or public-folder item.
    pub(crate) fn owner(&self) -> Option<&str> {
        match self {
            Self::Primary(_) | Self::Public { .. } => None,
            Self::Foreign { mailbox, .. } => Some(mailbox),
        }
    }

    /// The public folder this item lives in, when it is a public-folder
    /// item. `Some(_)` is exactly the discriminator that routes a read onto
    /// the EWS arm instead of the Graph REST arm.
    pub(crate) fn public_folder(&self) -> Option<&str> {
        match self {
            Self::Public { folder, .. } => Some(folder),
            _ => None,
        }
    }
}

/// Encode a `(public folder, native EWS item id)` pair into the `ObjectId`
/// the public-folder inventory / poll projections emit. The folder rides
/// along so hydration and attachment reads can look its routing headers up
/// in `routing_map` - without it a public-folder item would hydrate through
/// Graph REST, which cannot address a raw EWS `ItemId` at all.
pub(crate) fn encode_public_item_id(folder: &FolderId, item: &str) -> ObjectId {
    ObjectId(format!("{}{PUBLIC_SEP}{item}", folder.0))
}

/// Encode a `(scope, native message id)` pair into the `ObjectId` carried
/// out of the inventory / changes projection. When the scope is a foreign
/// (shared/delegate) folder, the owning mailbox is prefixed so later reads
/// route to `/users/{mailbox}`; a primary scope yields a bare native id.
pub(crate) fn encode_message_id(scope: &CursorScope, native: &str) -> ObjectId {
    match scope {
        CursorScope::FolderType { folder, .. } | CursorScope::Folder(folder) => {
            match parse_folder(folder).foreign() {
                Some(foreign) => ObjectId(format!("{}{FOREIGN_SEP}{native}", foreign.mailbox)),
                None => ObjectId(native.to_string()),
            }
        }
        _ => ObjectId(native.to_string()),
    }
}

/// Parse an `ObjectId` back into a `ParsedMessageId`. The public-folder form
/// is checked first (its `PUBLIC_SEP` is the more specific marker), then the
/// foreign-mailbox form; an id with neither separator is a primary-mailbox
/// item. Both splits take the FIRST separator only.
pub(crate) fn parse_message_id(id: &ObjectId) -> ParsedMessageId {
    if let Some((folder, item)) = id.0.split_once(PUBLIC_SEP) {
        return ParsedMessageId::Public {
            folder: folder.to_string(),
            item: item.to_string(),
        };
    }
    match id.0.split_once(FOREIGN_SEP) {
        Some((mailbox, message)) => ParsedMessageId::Foreign {
            mailbox: mailbox.to_string(),
            message: message.to_string(),
        },
        None => ParsedMessageId::Primary(id.0.clone()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn foreign_round_trips_through_folder_id() {
        let id = encode_foreign("shared@contoso.com", "AAMkAGI2");
        match parse_folder(&id) {
            ParsedFolder::Foreign(ForeignFolder { mailbox, folder }) => {
                assert_eq!(mailbox, "shared@contoso.com");
                assert_eq!(folder, "AAMkAGI2");
            }
            other => panic!("expected foreign, got {other:?}"),
        }
    }

    #[test]
    fn foreign_native_id_strips_prefix() {
        let id = encode_foreign("shared@contoso.com", "AAMkAGI2");
        let parsed = parse_folder(&id);
        assert_eq!(parsed.native_id(), "AAMkAGI2");
        assert!(parsed.foreign().is_some());
    }

    #[test]
    fn plain_id_is_primary() {
        let parsed = parse_folder(&FolderId("inbox".to_string()));
        assert_eq!(parsed, ParsedFolder::Primary("inbox".to_string()));
        assert_eq!(parsed.native_id(), "inbox");
        assert!(parsed.foreign().is_none());
    }

    #[test]
    fn owner_tag_is_mailbox_membership() {
        assert_eq!(
            owner_tag("shared@contoso.com"),
            MembershipScope::Mailbox(MailboxId("shared@contoso.com".to_string()))
        );
    }

    #[test]
    fn message_id_encodes_foreign_scope_and_round_trips() {
        let scope = CursorScope::FolderType {
            folder: encode_foreign("shared@contoso.com", "AAMkfolder"),
            ty: bifrost_types::ObjectType::Email,
        };
        let id = encode_message_id(&scope, "AAMkmessage");
        // The encoded id carries the owning mailbox, not the folder.
        assert_eq!(id.0, format!("shared@contoso.com{FOREIGN_SEP}AAMkmessage"));
        let parsed = parse_message_id(&id);
        assert_eq!(parsed.native_id(), "AAMkmessage");
        assert_eq!(parsed.owner(), Some("shared@contoso.com"));
        assert_eq!(
            parsed,
            ParsedMessageId::Foreign {
                mailbox: "shared@contoso.com".to_string(),
                message: "AAMkmessage".to_string(),
            }
        );
    }

    #[test]
    fn message_id_leaves_primary_scope_bare() {
        let scope = CursorScope::FolderType {
            folder: FolderId("inbox".to_string()),
            ty: bifrost_types::ObjectType::Email,
        };
        let id = encode_message_id(&scope, "AAMkmessage");
        assert_eq!(id.0, "AAMkmessage");
        let parsed = parse_message_id(&id);
        assert_eq!(parsed, ParsedMessageId::Primary("AAMkmessage".to_string()));
        assert_eq!(parsed.native_id(), "AAMkmessage");
        assert_eq!(parsed.owner(), None);
    }

    #[test]
    fn message_id_round_trip_is_byte_stable() {
        // Re-encoding the decoded native id under the same scope yields
        // the identical bytes - invariant 1 (one encoding per item).
        let scope = CursorScope::FolderType {
            folder: encode_foreign("shared@contoso.com", "AAMkfolder"),
            ty: bifrost_types::ObjectType::Email,
        };
        let once = encode_message_id(&scope, "AAMkmessage");
        let twice = encode_message_id(&scope, parse_message_id(&once).native_id());
        assert_eq!(once, twice);
    }

    #[test]
    fn public_item_id_round_trips_and_is_distinct_from_foreign() {
        let folder = FolderId("AAMkPF=".to_string());
        let id = encode_public_item_id(&folder, "AAMkItem=");
        assert_eq!(id.0, format!("AAMkPF={PUBLIC_SEP}AAMkItem="));

        let parsed = parse_message_id(&id);
        assert_eq!(parsed.native_id(), "AAMkItem=");
        assert_eq!(parsed.public_folder(), Some("AAMkPF="));
        // A public item has no owning mailbox: its routing comes from the
        // folder's `routing_map` entry, not from a `/users/{mailbox}` key.
        assert_eq!(parsed.owner(), None);

        // A foreign-mailbox id is NOT mistaken for a public one, and vice
        // versa.
        let foreign = encode_message_id(
            &CursorScope::FolderType {
                folder: encode_foreign("shared@contoso.com", "AAMkfolder"),
                ty: bifrost_types::ObjectType::Email,
            },
            "AAMkmessage",
        );
        assert_eq!(parse_message_id(&foreign).public_folder(), None);
        assert_eq!(
            parse_message_id(&foreign).owner(),
            Some("shared@contoso.com")
        );

        // Re-encoding a decoded public id is byte-stable.
        let twice = encode_public_item_id(&folder, parse_message_id(&id).native_id());
        assert_eq!(id, twice);
    }

    #[test]
    fn bare_message_id_parses_as_primary() {
        let parsed = parse_message_id(&ObjectId("AAMkmessage".to_string()));
        assert_eq!(parsed, ParsedMessageId::Primary("AAMkmessage".to_string()));
        assert_eq!(parsed.owner(), None);
    }

    #[test]
    fn folder_id_with_separator_in_native_part_splits_on_first() {
        // A native folder id can never contain US, but guard the split
        // semantics regardless: only the first separator delimits.
        let id = FolderId(format!("box{FOREIGN_SEP}a{FOREIGN_SEP}b"));
        match parse_folder(&id) {
            ParsedFolder::Foreign(ForeignFolder { mailbox, folder }) => {
                assert_eq!(mailbox, "box");
                assert_eq!(folder, format!("a{FOREIGN_SEP}b"));
            }
            other => panic!("expected foreign, got {other:?}"),
        }
    }
}
