pub mod get;
pub mod helpers;
pub mod set;

use std::fmt::Display;

use crate::core::set::skip_if_empty_list;
use crate::{email::EmailAddress, Get};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Identity<State = Get> {
    #[serde(skip)]
    _create_id: Option<usize>,

    #[serde(skip)]
    _state: std::marker::PhantomData<State>,

    #[serde(rename = "id")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,

    #[serde(rename = "name")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,

    #[serde(rename = "email")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub email: Option<String>,

    #[serde(rename = "replyTo")]
    #[serde(skip_serializing_if = "skip_if_empty_list")]
    pub reply_to: Option<Vec<EmailAddress>>,

    #[serde(rename = "bcc")]
    #[serde(skip_serializing_if = "skip_if_empty_list")]
    pub bcc: Option<Vec<EmailAddress>>,

    #[serde(rename = "textSignature")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub text_signature: Option<String>,

    #[serde(rename = "htmlSignature")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub html_signature: Option<String>,

    #[serde(rename = "mayDelete")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub may_delete: Option<bool>,
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

crate::impl_jmap_object!(Identity<State>, Property, true);

use crate::Set;

// Method structs for the new architecture
crate::define_get_method!(IdentityGet, Identity<Set>, "Identity/get", crate::core::capability::Submission, crate::core::get::GetResponse<Identity<Get>>);
crate::define_set_method!(IdentitySet, Identity<Set>, "Identity/set", crate::core::capability::Submission, crate::core::set::SetResponse<Identity<Get>>);
crate::define_changes_method!(IdentityChanges, "Identity/changes", crate::core::capability::Submission, crate::core::changes::ChangesResponse<Identity<Get>>);
