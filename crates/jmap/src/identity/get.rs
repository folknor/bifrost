use crate::email::EmailAddress;

use super::{Identity, IdentityId};

impl Identity {
    pub fn id(&self) -> Option<&IdentityId> {
        self.id.as_ref()
    }

    pub fn take_id(&mut self) -> IdentityId {
        self.id.take().unwrap_or_else(|| IdentityId::new(""))
    }

    pub fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }

    pub fn email(&self) -> Option<&str> {
        self.email.as_deref()
    }

    pub fn reply_to(&self) -> Option<&[EmailAddress]> {
        self.reply_to.as_deref()
    }

    pub fn bcc(&self) -> Option<&[EmailAddress]> {
        self.bcc.as_deref()
    }

    pub fn text_signature(&self) -> Option<&str> {
        self.text_signature.as_deref()
    }

    pub fn html_signature(&self) -> Option<&str> {
        self.html_signature.as_deref()
    }

    pub fn may_delete(&self) -> bool {
        self.may_delete.unwrap_or(false)
    }
}
