//! Mail composition: send, attachments, drafts.
//!
//! `SendRequest` is the top-level shape `send_message` accepts.
//! `DraftPatch` is the partial-update shape used by the draft
//! lifecycle. `AttachmentHandle` and `DraftHandle` are opaque
//! provider-minted ids returned by their respective primitives.

use bytes::Bytes;

use crate::ids::{MailboxId, ObjectId};

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
    /// `Content-ID` (without angle brackets) to stamp on the rendered
    /// MIME part. `Some(cid)` makes a `cid:<cid>` reference in the HTML
    /// body resolve to this part, so an inline image survives the send;
    /// `None` emits no `Content-ID` header. The renderer wraps the value
    /// in angle brackets (`Content-ID: <cid>`) per RFC 2045 §7.
    pub content_id: Option<String>,
}

/// Send-as / send-on-behalf-of identity for a shared or delegate
/// mailbox. `None` on `SendRequest::send_as` is an ordinary personal
/// send. Distinct from `SendRequest::from` (the author/From *header*):
/// `send_as` selects the *sending mailbox / API path*, which on some
/// providers (Microsoft Graph) is a routing dimension separate from
/// the From header. Gated by `PimMethodSupport.send_as`; a request
/// carrying `Some(..)` on a provider with `send_as == false` is
/// rejected `Unsupported(Send)`, never silently sent from the
/// authenticated user's own mailbox.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum SendAs {
    /// Send **as** the shared mailbox: the mailbox is both the author
    /// (`From`) and the sender. The authenticated user must hold
    /// Send-As rights on it; the recipient sees only the shared
    /// mailbox. `As` *forces* `from` to the mailbox - any
    /// `SendRequest::from` the consumer set is overridden, because the
    /// whole contract of `As` is "the recipient sees only the shared
    /// mailbox." A consumer that wants `from` to diverge from the
    /// sending mailbox wants `OnBehalfOf`, not `As`.
    As(MailboxId),
    /// Send **on behalf of** the shared mailbox: when the consumer does not
    /// supply `from`, the mailbox is the author (`From`); the authenticated
    /// user is the sender (`Sender`). A provider may preserve an explicit
    /// consumer `from`. The recipient sees "user on behalf of mailbox".
    OnBehalfOf(MailboxId),
}

impl SendAs {
    /// The shared mailbox identity, regardless of mode - the
    /// `/users/{id}` routing key.
    #[must_use]
    pub fn mailbox(&self) -> &MailboxId {
        match self {
            Self::As(m) | Self::OnBehalfOf(m) => m,
        }
    }
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
    /// Requested server-side send time. `None` sends immediately.
    /// `Some(t)` requests delayed delivery where the provider supports
    /// it (`capabilities().pim_methods.scheduled_send`); on providers
    /// that do not, a `Some(t)` request is rejected with
    /// `Unsupported(Send)` rather than silently sent now. `t` is an
    /// absolute wall-clock instant; the provider boundary validates it
    /// is in the future and within the provider's max-delay window.
    pub scheduled: Option<std::time::SystemTime>,
    /// Send-as / send-on-behalf-of a shared or delegate mailbox.
    /// `None` is an ordinary personal send. See `SendAs`. Honored only
    /// where `capabilities().pim_methods.send_as` is `true` (Graph or JMAP
    /// with a foreign submission-capable account);
    /// `Some(..)` elsewhere is rejected `Unsupported(Send)`.
    pub send_as: Option<SendAs>,
    /// Request a read receipt (message disposition notification) for this
    /// send. `true` makes each provider ask the recipient's MUA to confirm
    /// the message was displayed: the SMTP/JMAP/IMAP/Gmail assemblers emit
    /// a `Disposition-Notification-To` header (RFC 8098) targeting the
    /// resolved `from` address, and Graph sets `isReadReceiptRequested`.
    /// Defaults to `false` so existing callers are unaffected. Whether the
    /// recipient honors the request is out of the sender's control on every
    /// provider.
    pub request_read_receipt: bool,
}

/// Validate a requested scheduled-send instant at the provider
/// boundary. Shared so the past-instant and over-window rules are
/// identical across every native provider.
///
/// - `at <= now()` (past or present instant) -> `Request(Malformed)`.
/// - `at - now() > max_window` (when `max_window` is `Some`) ->
///   `Request(Malformed)`. Providers with no documented hard cap pass
///   `None` and rely on server rejection.
///
/// Returns `Ok(())` for a valid future instant inside the window.
///
/// # Errors
///
/// Returns `Request(Malformed)` when `at` is not strictly in the
/// future, or when it exceeds `max_window` past now.
pub fn validate_scheduled(
    at: std::time::SystemTime,
    max_window: Option<std::time::Duration>,
) -> Result<(), crate::error::AccountError> {
    use crate::error::{
        AccountErrorBuilder, AccountErrorKind, Cause, DiagnosticText, RequestCause,
        RequestErrorKind,
    };

    let now = std::time::SystemTime::now();
    let malformed = |detail: &'static str| {
        AccountErrorBuilder::new(
            AccountErrorKind::Request(RequestErrorKind::Malformed),
            Cause::Request(RequestCause::Malformed {
                detail: DiagnosticText::user_safe(detail),
            }),
        )
        .operation(crate::error::AccountOperation::Send)
        .try_build()
        .expect("valid account error classification")
    };

    let delta = match at.duration_since(now) {
        // Strictly-future requirement: `Ok(0)` (exactly now) and the
        // `Err` arm (at < now) are both rejected as past instants.
        Ok(d) if !d.is_zero() => d,
        _ => return Err(malformed("Scheduled send time must be in the future.")),
    };

    if let Some(window) = max_window
        && delta > window
    {
        return Err(malformed(
            "Scheduled send time exceeds the provider's maximum delay window.",
        ));
    }

    Ok(())
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

#[cfg(test)]
mod scheduled_send_tests {
    use std::time::{Duration, SystemTime};

    use super::{SendRequest, validate_scheduled};
    use crate::capabilities::PimMethodSupport;
    use crate::error::{AccountErrorKind, AccountOperation, RequestErrorKind};

    #[test]
    fn scheduled_send_default_request_is_immediate() {
        assert!(SendRequest::default().scheduled.is_none());
    }

    #[test]
    fn scheduled_send_capability_defaults_false() {
        assert!(!PimMethodSupport::default().scheduled_send);
    }

    #[test]
    fn scheduled_send_idempotency_placement() {
        // Both scheduled-send mutators are non-idempotent: an in-flight
        // drop on either must reconcile, not blind-retry. (Cancel was
        // previously treated idempotent, diverging from its RescheduleSend
        // sibling and from the wider destructive-op family.)
        assert!(!AccountOperation::CancelScheduledSend.is_idempotent());
        assert!(!AccountOperation::RescheduleSend.is_idempotent());
    }

    #[test]
    fn scheduled_send_validate_rejects_past_instant() {
        let past = SystemTime::now() - Duration::from_secs(60);
        let err = validate_scheduled(past, None).expect_err("past instant must be rejected");
        assert_eq!(
            err.kind(),
            &AccountErrorKind::Request(RequestErrorKind::Malformed)
        );
        assert_eq!(err.operation(), Some(AccountOperation::Send));
    }

    #[test]
    fn scheduled_send_validate_rejects_over_window() {
        let future = SystemTime::now() + Duration::from_secs(3600);
        let err = validate_scheduled(future, Some(Duration::from_secs(60)))
            .expect_err("over-window instant must be rejected");
        assert_eq!(
            err.kind(),
            &AccountErrorKind::Request(RequestErrorKind::Malformed)
        );
    }

    #[test]
    fn scheduled_send_validate_accepts_valid_future() {
        let future = SystemTime::now() + Duration::from_secs(120);
        assert!(validate_scheduled(future, Some(Duration::from_secs(3600))).is_ok());
        assert!(validate_scheduled(future, None).is_ok());
    }
}

#[cfg(test)]
mod send_as_tests {
    use super::{SendAs, SendRequest};
    use crate::capabilities::PimMethodSupport;
    use crate::ids::MailboxId;

    #[test]
    fn send_as_defaults_none() {
        assert!(SendRequest::default().send_as.is_none());
    }

    #[test]
    fn send_as_capability_defaults_false() {
        assert!(!PimMethodSupport::default().send_as);
    }

    #[test]
    fn send_as_mailbox_accessor() {
        let mailbox = MailboxId("shared@contoso.com".to_string());
        assert_eq!(SendAs::As(mailbox.clone()).mailbox(), &mailbox);
        assert_eq!(SendAs::OnBehalfOf(mailbox.clone()).mailbox(), &mailbox);
    }
}
