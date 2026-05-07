pub mod get;
pub mod set;

use std::fmt::Display;

use crate::core::set::skip_if_empty_list;
use crate::email::EmailAddress;
use serde::{Deserialize, Serialize};

mod marker {
    pub enum Identity {}
}
/// Strongly-typed Identity ID.
pub type IdentityId = crate::core::id::Id<marker::Identity>;

/// Server-returned Identity object (RFC 8621 §6).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Identity {
    #[serde(rename = "id")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) id: Option<String>,

    #[serde(rename = "name")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) name: Option<String>,

    #[serde(rename = "email")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) email: Option<String>,

    #[serde(rename = "replyTo")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) reply_to: Option<Vec<EmailAddress>>,

    #[serde(rename = "bcc")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) bcc: Option<Vec<EmailAddress>>,

    #[serde(rename = "textSignature")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) text_signature: Option<String>,

    #[serde(rename = "htmlSignature")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) html_signature: Option<String>,

    #[serde(rename = "mayDelete")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) may_delete: Option<bool>,
}

#[derive(Debug, Clone, Serialize)]
pub struct IdentityCreate {
    #[serde(skip)]
    pub(super) _create_id: Option<usize>,

    #[serde(rename = "name")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) name: Option<String>,

    #[serde(rename = "email")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) email: Option<String>,

    #[serde(rename = "replyTo")]
    #[serde(skip_serializing_if = "skip_if_empty_list")]
    pub(super) reply_to: Option<Vec<EmailAddress>>,

    #[serde(rename = "bcc")]
    #[serde(skip_serializing_if = "skip_if_empty_list")]
    pub(super) bcc: Option<Vec<EmailAddress>>,

    #[serde(rename = "textSignature")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) text_signature: Option<String>,

    #[serde(rename = "htmlSignature")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) html_signature: Option<String>,
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct IdentityPatch {
    #[serde(rename = "name")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) name: Option<String>,

    #[serde(rename = "replyTo")]
    #[serde(skip_serializing_if = "skip_if_empty_list")]
    pub(super) reply_to: Option<Vec<EmailAddress>>,

    #[serde(rename = "bcc")]
    #[serde(skip_serializing_if = "skip_if_empty_list")]
    pub(super) bcc: Option<Vec<EmailAddress>>,

    #[serde(rename = "textSignature")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) text_signature: Option<String>,

    #[serde(rename = "htmlSignature")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) html_signature: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash, Copy)]
#[non_exhaustive]
pub enum Property {
    #[serde(rename = "id")]
    Id,
    #[serde(rename = "name")]
    Name,
    #[serde(rename = "email")]
    Email,
    #[serde(rename = "replyTo")]
    ReplyTo,
    #[serde(rename = "bcc")]
    Bcc,
    #[serde(rename = "textSignature")]
    TextSignature,
    #[serde(rename = "htmlSignature")]
    HtmlSignature,
    #[serde(rename = "mayDelete")]
    MayDelete,
}

impl Display for Property {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Property::Id => write!(f, "id"),
            Property::Name => write!(f, "name"),
            Property::Email => write!(f, "email"),
            Property::ReplyTo => write!(f, "replyTo"),
            Property::Bcc => write!(f, "bcc"),
            Property::TextSignature => write!(f, "textSignature"),
            Property::HtmlSignature => write!(f, "htmlSignature"),
            Property::MayDelete => write!(f, "mayDelete"),
        }
    }
}

impl crate::core::Object for Identity {
    type Property = Property;
    fn requires_account_id() -> bool {
        true
    }
}

impl crate::core::changes::ChangesObject for Identity {
    type ChangesResponse = ();
}

impl crate::core::get::GetObject for Identity {
    type GetArguments = ();
}

impl crate::core::set::SetObject for Identity {
    type Create = IdentityCreate;
    type Patch = IdentityPatch;
    type SetArguments = ();
}

impl crate::core::SetCreate for IdentityCreate {
    fn create_id(&self) -> Option<String> {
        self._create_id.map(|id| format!("c{id}"))
    }

    fn new(create_id: Option<usize>) -> Self {
        IdentityCreate {
            _create_id: create_id,
            name: None,
            email: None,
            reply_to: Vec::with_capacity(0).into(),
            bcc: Vec::with_capacity(0).into(),
            text_signature: None,
            html_signature: None,
        }
    }
}

crate::define_get_method!(
    IdentityGet,
    Identity,
    "Identity/get",
    crate::core::capability::Submission
);
crate::define_set_method!(
    IdentitySet,
    Identity,
    "Identity/set",
    crate::core::capability::Submission
);
crate::define_changes_method!(
    IdentityChanges,
    Identity,
    "Identity/changes",
    crate::core::capability::Submission
);
