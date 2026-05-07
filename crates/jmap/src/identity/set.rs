use crate::email::EmailAddress;

use super::{IdentityCreate, IdentityPatch};

impl IdentityCreate {
    pub fn name(&mut self, name: impl Into<String>) -> &mut Self {
        self.name = Some(name.into());
        self
    }

    pub fn email(&mut self, email: impl Into<String>) -> &mut Self {
        self.email = Some(email.into());
        self
    }

    pub fn bcc<T, U>(&mut self, bcc: Option<T>) -> &mut Self
    where
        T: Iterator<Item = U>,
        U: Into<EmailAddress>,
    {
        self.bcc = bcc.map(|s| s.map(std::convert::Into::into).collect());
        self
    }

    pub fn reply_to<T, U>(&mut self, reply_to: Option<T>) -> &mut Self
    where
        T: Iterator<Item = U>,
        U: Into<EmailAddress>,
    {
        self.reply_to = reply_to.map(|s| s.map(std::convert::Into::into).collect());
        self
    }

    pub fn text_signature(&mut self, text_signature: impl Into<String>) -> &mut Self {
        self.text_signature = Some(text_signature.into());
        self
    }

    pub fn html_signature(&mut self, html_signature: impl Into<String>) -> &mut Self {
        self.html_signature = Some(html_signature.into());
        self
    }
}

impl IdentityPatch {
    pub fn name(&mut self, name: impl Into<String>) -> &mut Self {
        self.name = Some(name.into());
        self
    }

    pub fn bcc<T, U>(&mut self, bcc: Option<T>) -> &mut Self
    where
        T: Iterator<Item = U>,
        U: Into<EmailAddress>,
    {
        self.bcc = bcc.map(|s| s.map(std::convert::Into::into).collect());
        self
    }

    pub fn reply_to<T, U>(&mut self, reply_to: Option<T>) -> &mut Self
    where
        T: Iterator<Item = U>,
        U: Into<EmailAddress>,
    {
        self.reply_to = reply_to.map(|s| s.map(std::convert::Into::into).collect());
        self
    }

    pub fn text_signature(&mut self, text_signature: impl Into<String>) -> &mut Self {
        self.text_signature = Some(text_signature.into());
        self
    }

    pub fn html_signature(&mut self, html_signature: impl Into<String>) -> &mut Self {
        self.html_signature = Some(html_signature.into());
        self
    }
}
