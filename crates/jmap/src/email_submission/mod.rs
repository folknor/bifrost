pub mod get;
pub mod query;
pub mod set;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::Display;

use crate::email::{EmailId, EmailPatch};
use crate::identity::IdentityId;
use crate::thread::ThreadId;

mod marker {
    pub enum EmailSubmission {}
}
/// Strongly-typed EmailSubmission ID.
pub type EmailSubmissionId = crate::core::id::Id<marker::EmailSubmission>;

#[derive(Debug, Clone, Serialize, Default)]
pub struct SetArguments {
    /// Patches to apply to the referenced email on successful submit,
    /// keyed by `EmailSubmission` create-id (e.g. "c0") or by real
    /// `EmailSubmission` id with `#` prefix per RFC 8621.
    #[serde(rename = "onSuccessUpdateEmail")]
    #[serde(skip_serializing_if = "Option::is_none")]
    on_success_update_email: Option<HashMap<String, EmailPatch>>,
    #[serde(rename = "onSuccessDestroyEmail")]
    #[serde(skip_serializing_if = "Option::is_none")]
    on_success_destroy_email: Option<Vec<String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EmailSubmission {
    #[serde(rename = "id")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) id: Option<EmailSubmissionId>,

    #[serde(rename = "identityId")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) identity_id: Option<IdentityId>,

    #[serde(rename = "emailId")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) email_id: Option<EmailId>,

    #[serde(rename = "threadId")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) thread_id: Option<ThreadId>,

    #[serde(rename = "envelope")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) envelope: Option<Envelope>,

    #[serde(rename = "sendAt")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) send_at: Option<DateTime<Utc>>,

    #[serde(rename = "undoStatus")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) undo_status: Option<UndoStatus>,

    #[serde(rename = "deliveryStatus")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) delivery_status: Option<HashMap<String, DeliveryStatus>>,

    #[serde(rename = "dsnBlobIds")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) dsn_blob_ids: Option<Vec<crate::core::id::BlobId>>,

    #[serde(rename = "mdnBlobIds")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) mdn_blob_ids: Option<Vec<crate::core::id::BlobId>>,
}

#[derive(Debug, Clone, Serialize)]
pub struct EmailSubmissionCreate {
    #[serde(skip)]
    pub(super) _create_id: Option<usize>,

    #[serde(rename = "identityId")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) identity_id: Option<IdentityId>,

    #[serde(rename = "emailId")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) email_id: Option<EmailId>,

    #[serde(rename = "envelope")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) envelope: Option<Envelope>,

    #[serde(rename = "undoStatus")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) undo_status: Option<UndoStatus>,
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct EmailSubmissionPatch {
    #[serde(rename = "undoStatus")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) undo_status: Option<UndoStatus>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Envelope {
    #[serde(rename = "mailFrom")]
    pub(super) mail_from: Address,

    #[serde(rename = "rcptTo")]
    pub(super) rcpt_to: Vec<Address>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Address {
    pub(super) email: String,
    pub(super) parameters: Option<HashMap<String, Option<String>>>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[non_exhaustive]
pub enum UndoStatus {
    #[serde(rename = "pending")]
    Pending,
    #[serde(rename = "final")]
    Final,
    #[serde(rename = "canceled")]
    Canceled,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DeliveryStatus {
    #[serde(rename = "smtpReply")]
    smtp_reply: String,

    #[serde(rename = "delivered")]
    delivered: Delivered,

    #[serde(rename = "displayed")]
    displayed: Displayed,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[non_exhaustive]
pub enum Delivered {
    #[serde(rename = "queued")]
    Queued,
    #[serde(rename = "yes")]
    Yes,
    #[serde(rename = "no")]
    No,
    #[serde(rename = "unknown")]
    Unknown,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[non_exhaustive]
pub enum Displayed {
    #[serde(rename = "unknown")]
    Unknown,
    #[serde(rename = "yes")]
    Yes,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash, Copy)]
#[non_exhaustive]
pub enum Property {
    #[serde(rename = "id")]
    Id,
    #[serde(rename = "identityId")]
    IdentityId,
    #[serde(rename = "emailId")]
    EmailId,
    #[serde(rename = "threadId")]
    ThreadId,
    #[serde(rename = "envelope")]
    Envelope,
    #[serde(rename = "sendAt")]
    SendAt,
    #[serde(rename = "undoStatus")]
    UndoStatus,
    #[serde(rename = "deliveryStatus")]
    DeliveryStatus,
    #[serde(rename = "dsnBlobIds")]
    DsnBlobIds,
    #[serde(rename = "mdnBlobIds")]
    MdnBlobIds,
}

impl Display for Property {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Property::Id => write!(f, "id"),
            Property::IdentityId => write!(f, "identityId"),
            Property::EmailId => write!(f, "emailId"),
            Property::ThreadId => write!(f, "threadId"),
            Property::Envelope => write!(f, "envelope"),
            Property::SendAt => write!(f, "sendAt"),
            Property::UndoStatus => write!(f, "undoStatus"),
            Property::DeliveryStatus => write!(f, "deliveryStatus"),
            Property::DsnBlobIds => write!(f, "dsnBlobIds"),
            Property::MdnBlobIds => write!(f, "mdnBlobIds"),
        }
    }
}

impl crate::core::Object for EmailSubmission {
    type Property = Property;
    type Id = EmailSubmissionId;
    fn requires_account_id() -> bool {
        true
    }
}

impl crate::core::changes::ChangesObject for EmailSubmission {
    type ChangesResponse = ();
}

impl crate::core::get::GetObject for EmailSubmission {
    type GetArguments = ();
}

impl crate::core::set::SetObject for EmailSubmission {
    type Create = EmailSubmissionCreate;
    type Patch = EmailSubmissionPatch;
    type SetArguments = SetArguments;
}

impl crate::core::SetCreate for EmailSubmissionCreate {
    fn create_id(&self) -> Option<String> {
        self._create_id.map(|id| format!("c{id}"))
    }

    fn new(create_id: Option<usize>) -> Self {
        EmailSubmissionCreate {
            _create_id: create_id,
            identity_id: None,
            email_id: None,
            envelope: None,
            undo_status: None,
        }
    }
}

crate::define_get_method!(
    EmailSubmissionGet,
    EmailSubmission,
    "EmailSubmission/get",
    crate::core::capability::Submission
);
crate::define_set_method!(
    EmailSubmissionSet,
    EmailSubmission,
    "EmailSubmission/set",
    crate::core::capability::Submission
);
crate::define_changes_method!(
    EmailSubmissionChanges,
    EmailSubmission,
    "EmailSubmission/changes",
    crate::core::capability::Submission
);
crate::define_query_method!(
    EmailSubmissionQuery,
    EmailSubmission,
    "EmailSubmission/query",
    crate::core::capability::Submission
);
crate::define_query_changes_method!(
    EmailSubmissionQueryChanges,
    EmailSubmission,
    "EmailSubmission/queryChanges",
    crate::core::capability::Submission
);
