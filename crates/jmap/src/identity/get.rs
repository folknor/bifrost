use crate::email::EmailAddress;

use super::{Identity, IdentityId};

impl Identity {
    pub(crate) fn id(&self) -> Option<&IdentityId> {
        self.id.as_ref()
    }

    pub(crate) fn take_id(&mut self) -> IdentityId {
        self.id.take().unwrap_or_else(|| IdentityId::new(""))
    }

    pub(crate) fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }

    pub(crate) fn email(&self) -> Option<&str> {
        self.email.as_deref()
    }

    pub(crate) fn reply_to(&self) -> Option<&[EmailAddress]> {
        self.reply_to.as_deref()
    }

    pub(crate) fn bcc(&self) -> Option<&[EmailAddress]> {
        self.bcc.as_deref()
    }

    pub(crate) fn text_signature(&self) -> Option<&str> {
        self.text_signature.as_deref()
    }

    pub(crate) fn html_signature(&self) -> Option<&str> {
        self.html_signature.as_deref()
    }

    pub(crate) fn may_delete(&self) -> bool {
        self.may_delete.unwrap_or(false)
    }
}
