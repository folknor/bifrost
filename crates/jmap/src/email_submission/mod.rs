pub mod get;
pub mod query;
pub mod set;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::Display;

use crate::{Get, Set, email::Email};

mod marker {
    pub enum EmailSubmission {}
}
/// Strongly-typed EmailSubmission ID.
pub type EmailSubmissionId = crate::core::id::Id<marker::EmailSubmission>;

#[derive(Debug, Clone, Serialize, Default)]
pub struct SetArguments {
    #[serde(rename = "onSuccessUpdateEmail")]
    #[serde(skip_serializing_if = "Option::is_none")]
    on_success_update_email: Option<HashMap<String, Email<Set>>>,
    #[serde(rename = "onSuccessDestroyEmail")]
    #[serde(skip_serializing_if = "Option::is_none")]
    on_success_destroy_email: Option<Vec<String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EmailSubmission<State = Get> {
    #[serde(skip)]
    _create_id: Option<usize>,

    #[serde(skip)]
    _state: std::marker::PhantomData<State>,

    #[serde(rename = "id")]
    #[serde(skip_serializing_if = "Option::is_none")]
    id: Option<String>,

    #[serde(rename = "identityId")]
    #[serde(skip_serializing_if = "Option::is_none")]
    identity_id: Option<String>,

    #[serde(rename = "emailId")]
    #[serde(skip_serializing_if = "Option::is_none")]
    email_id: Option<String>,

    #[serde(rename = "threadId")]
    #[serde(skip_serializing_if = "Option::is_none")]
    thread_id: Option<String>,

    #[serde(rename = "envelope")]
    #[serde(skip_serializing_if = "Option::is_none")]
    envelope: Option<Envelope>,

    #[serde(rename = "sendAt")]
    #[serde(skip_serializing_if = "Option::is_none")]
    send_at: Option<DateTime<Utc>>,

    #[serde(rename = "undoStatus")]
    #[serde(skip_serializing_if = "Option::is_none")]
    undo_status: Option<UndoStatus>,

    #[serde(rename = "deliveryStatus")]
    #[serde(skip_serializing_if = "Option::is_none")]
    delivery_status: Option<HashMap<String, DeliveryStatus>>,

    #[serde(rename = "dsnBlobIds")]
    #[serde(skip_serializing_if = "Option::is_none")]
    dsn_blob_ids: Option<Vec<String>>,

    #[serde(rename = "mdnBlobIds")]
    #[serde(skip_serializing_if = "Option::is_none")]
    mdn_blob_ids: Option<Vec<String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Envelope {
    #[serde(rename = "mailFrom")]
    mail_from: Address,

    #[serde(rename = "rcptTo")]
    rcpt_to: Vec<Address>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Address<State = Get> {
    #[serde(skip)]
    _state: std::marker::PhantomData<State>,

    email: String,
    parameters: Option<HashMap<String, Option<String>>>,
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

crate::impl_jmap_object!(EmailSubmission<State>, Property, true);

// Note: EmailSubmission needs both Mail and Submission capabilities.
// The macros only support one capability, so we use Submission here.
// The Mail capability will be auto-added when needed through the request.

// Method structs for the new architecture
crate::define_get_method!(
    EmailSubmissionGet,
    EmailSubmission<Set>,
    "EmailSubmission/get",
    crate::core::capability::Submission,
    crate::core::get::GetResponse<EmailSubmission<Get>>
);
crate::define_set_method!(
    EmailSubmissionSet,
    EmailSubmission<Set>,
    "EmailSubmission/set",
    crate::core::capability::Submission,
    crate::core::set::SetResponse<EmailSubmission<Get>>
);
crate::define_changes_method!(
    EmailSubmissionChanges,
    "EmailSubmission/changes",
    crate::core::capability::Submission,
    crate::core::changes::ChangesResponse<EmailSubmission<Get>>
);
crate::define_query_method!(
    EmailSubmissionQuery,
    EmailSubmission<Set>,
    "EmailSubmission/query",
    crate::core::capability::Submission
);
crate::define_query_changes_method!(
    EmailSubmissionQueryChanges,
    EmailSubmission<Set>,
    "EmailSubmission/queryChanges",
    crate::core::capability::Submission
);
