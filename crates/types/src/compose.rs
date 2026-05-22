//! Mail composition: send, attachments, drafts.
//!
//! `SendRequest` is the top-level shape `send_message` accepts.
//! `DraftPatch` is the partial-update shape used by the draft
//! lifecycle. `AttachmentHandle` and `DraftHandle` are opaque
//! provider-minted ids returned by their respective primitives.

use bytes::Bytes;

use crate::ids::ObjectId;

/// Opaque handle to a server-side uploaded attachment.
///
/// `attachment_upload` returns one of these; subsequent
/// `draft_update` calls reference attachments by handle rather than
/// re-uploading the bytes. The handle is protocol-owned bytes;
/// consumers treat it as opaque.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct AttachmentHandle(pub String);

/// Opaque handle to a server-side draft.
///
/// `draft_create` returns one; `draft_update`, `draft_discard`, and
/// `draft_send` accept one. Distinct from `ObjectId` so the type
/// system catches mix-ups between "this is a draft handle" and "this
/// is a message id".
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DraftHandle(pub String);

impl From<DraftHandle> for ObjectId {
    fn from(d: DraftHandle) -> Self {
        Self(d.0)
    }
}

/// A single named recipient. `name` is `None` when only the address
/// was supplied; consumers render `name <address>` when both are
/// present and `<address>` when only the address is.
///
/// Not `#[non_exhaustive]` so both consumers (`SendRequest` /
/// `DraftPatch` builders) and protocol Account impls (returning
/// `Identity::reply_to`) can construct it freely.
#[derive(Debug, Clone)]
pub struct Address {
    pub name: Option<String>,
    pub address: String,
}

impl Address {
    /// Convenience constructor for an address-only entry.
    #[must_use]
    pub fn bare(address: impl Into<String>) -> Self {
        Self {
            name: None,
            address: address.into(),
        }
    }

    /// Convenience constructor for a named entry.
    #[must_use]
    pub fn named(name: impl Into<String>, address: impl Into<String>) -> Self {
        Self {
            name: Some(name.into()),
            address: address.into(),
        }
    }
}

/// In-band attachment for `SendRequest` / `DraftPatch`. For larger
/// payloads use `attachment_upload` and reference by
/// `AttachmentHandle` instead.
///
/// Not `#[non_exhaustive]` because consumers construct it on the
/// send / draft request side.
#[derive(Debug, Clone)]
pub struct AttachmentInline {
    /// Filename presented in the MIME `Content-Disposition`.
    pub filename: String,
    /// Content-Type.
    pub mime: String,
    /// Raw payload bytes.
    pub data: Bytes,
    /// Whether the consumer wants `Content-Disposition: inline`
    /// (preview-in-body) rather than `attachment`.
    pub inline: bool,
}

/// Top-level shape `Account::send_message` accepts.
///
/// `identity` selects which sending identity to attach (relevant for
/// accounts with multiple `Identity` rows from `identities_list`).
/// Most fields are `Option` so a minimal send can omit reply-to,
/// cc, bcc, in-reply-to, etc. without nesting one builder inside
/// another.
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct SendRequest {
    /// Sending identity. `None` selects the account's default.
    pub identity: Option<IdentityId>,
    /// From line. `None` uses the identity's address.
    pub from: Option<Address>,
    /// To recipients. At least one of `to`, `cc`, or `bcc` must be
    /// non-empty for the protocol to accept the send; this is
    /// enforced at the protocol layer, not in this type.
    pub to: Vec<Address>,
    pub cc: Vec<Address>,
    pub bcc: Vec<Address>,
    /// Reply-To header.
    pub reply_to: Vec<Address>,
    /// Subject line.
    pub subject: Option<String>,
    /// Plain-text body. Mutually permissive with `body_html`;
    /// providing both produces a multipart/alternative.
    pub body_text: Option<String>,
    /// HTML body.
    pub body_html: Option<String>,
    /// In-line attachments.
    pub attachments_inline: Vec<AttachmentInline>,
    /// Pre-uploaded attachment handles (from `attachment_upload`).
    pub attachments_uploaded: Vec<AttachmentHandle>,
    /// In-Reply-To and References headers (for threading).
    pub in_reply_to: Option<String>,
    pub references: Vec<String>,
    /// Whether the server should append the sent message to the
    /// Sent folder. `None` leaves the choice to the protocol's
    /// default. Honored by JMAP via `EmailSubmission/set`'s
    /// `onSuccessUpdateEmail`; Gmail and Graph append on send by
    /// default and ignore this when `Some(false)`; IMAP uses the
    /// configured `bifrost-smtp` transport plus an APPEND.
    pub save_to_sent: Option<bool>,
}

/// Partial-update shape used by `draft_create` and `draft_update`.
///
/// Every field is `Option`-shaped so a `draft_update` can change
/// just the body and leave the recipient list alone. Empty `Vec`s
/// in the optional collection fields (e.g. `to: Some(vec![])`)
/// explicitly clear the collection; `None` leaves it unchanged.
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct DraftPatch {
    pub identity: Option<IdentityId>,
    pub from: Option<Option<Address>>,
    pub to: Option<Vec<Address>>,
    pub cc: Option<Vec<Address>>,
    pub bcc: Option<Vec<Address>>,
    pub reply_to: Option<Vec<Address>>,
    pub subject: Option<Option<String>>,
    pub body_text: Option<Option<String>>,
    pub body_html: Option<Option<String>>,
    pub attachments_inline: Option<Vec<AttachmentInline>>,
    pub attachments_uploaded: Option<Vec<AttachmentHandle>>,
    pub in_reply_to: Option<Option<String>>,
    pub references: Option<Vec<String>>,
}

/// Engine-facing identity id. Distinct from `ObjectId` because
/// identities are a separate object class from messages and trying
/// to round-trip an identity id through `ObjectId` would lose the
/// signal that the id namespace is different.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct IdentityId(pub String);
