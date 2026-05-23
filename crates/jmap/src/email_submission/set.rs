use super::{
    Address, EmailSubmissionCreate, EmailSubmissionPatch, EmailSubmissionSet, Envelope,
    SetArguments, UndoStatus,
};
use crate::core::request::ResultReference;
use crate::email::{EmailId, EmailPatch};
use crate::identity::IdentityId;
use std::collections::HashMap;

impl EmailSubmissionCreate {
    pub fn identity_id(&mut self, identity_id: impl Into<IdentityId>) -> &mut Self {
        self.identity_id = Some(identity_id.into());
        self
    }

    pub fn email_id(&mut self, email_id: impl Into<EmailId>) -> &mut Self {
        self.email_id = Some(email_id.into());
        self.email_id_ref = None;
        self
    }

    pub fn email_id_ref(&mut self, reference: ResultReference) -> &mut Self {
        self.email_id = None;
        self.email_id_ref = Some(reference);
        self
    }

    pub fn envelope<S, T, U>(&mut self, mail_from: S, rcpt_to: T) -> &mut Self
    where
        S: Into<Address>,
        T: IntoIterator<Item = U>,
        U: Into<Address>,
    {
        self.envelope = Some(Envelope::new(mail_from, rcpt_to));
        self
    }

    pub fn undo_status(&mut self, undo_status: UndoStatus) -> &mut Self {
        self.undo_status = Some(undo_status);
        self
    }
}

impl EmailSubmissionPatch {
    pub fn undo_status(&mut self, undo_status: UndoStatus) -> &mut Self {
        self.undo_status = Some(undo_status);
        self
    }
}

impl Envelope {
    pub fn new<S, T, U>(mail_from: S, rcpt_to: T) -> Envelope
    where
        S: Into<Address>,
        T: IntoIterator<Item = U>,
        U: Into<Address>,
    {
        Envelope {
            mail_from: mail_from.into(),
            rcpt_to: rcpt_to.into_iter().map(std::convert::Into::into).collect(),
        }
    }
}

impl Address {
    pub fn new(email: impl Into<String>) -> Address {
        Address {
            email: email.into(),
            parameters: None,
        }
    }

    pub fn with_parameter(
        mut self,
        parameter: impl Into<String>,
        value: Option<impl Into<String>>,
    ) -> Self {
        self.parameters
            .get_or_insert_with(HashMap::new)
            .insert(parameter.into(), value.map(std::convert::Into::into));
        self
    }
}

impl From<String> for Address {
    fn from(email: String) -> Self {
        Address {
            email,
            parameters: None,
        }
    }
}

impl From<&str> for Address {
    fn from(email: &str) -> Self {
        Address {
            email: email.to_string(),
            parameters: None,
        }
    }
}

impl SetArguments {
    /// Reference an EmailSubmission by create-id ("c0"); the `#` prefix
    /// is added automatically per RFC 8621.
    pub fn on_success_update_email(&mut self, create_id: impl Into<String>) -> &mut EmailPatch {
        self.on_success_update_email_(format!("#{}", create_id.into()))
    }

    pub fn on_success_update_email_id(
        &mut self,
        id: impl Into<super::EmailSubmissionId>,
    ) -> &mut EmailPatch {
        self.on_success_update_email_(id.into().into_string())
    }

    fn on_success_update_email_(&mut self, id: impl Into<String>) -> &mut EmailPatch {
        let id = id.into();
        self.on_success_update_email
            .get_or_insert_with(HashMap::new)
            .insert(id.clone(), EmailPatch::default());
        self.on_success_update_email
            .as_mut()
            .unwrap()
            .get_mut(&id)
            .unwrap()
    }

    /// Reference an EmailSubmission by create-id ("c0"); the `#` prefix
    /// is added automatically.
    pub fn on_success_destroy_email(&mut self, create_id: impl Into<String>) -> &mut Self {
        self.on_success_destroy_email
            .get_or_insert_with(Vec::new)
            .push(format!("#{}", create_id.into()));
        self
    }

    pub fn on_success_destroy_email_id(
        &mut self,
        id: impl Into<super::EmailSubmissionId>,
    ) -> &mut Self {
        self.on_success_destroy_email
            .get_or_insert_with(Vec::new)
            .push(id.into().into_string());
        self
    }
}

impl EmailSubmissionSet {
    pub fn on_success_update_email(&mut self, create_id: impl Into<String>) -> &mut EmailPatch {
        self.arguments().on_success_update_email(create_id)
    }

    pub fn on_success_update_email_id(
        &mut self,
        id: impl Into<super::EmailSubmissionId>,
    ) -> &mut EmailPatch {
        self.arguments().on_success_update_email_id(id)
    }

    #[must_use]
    pub fn on_success_destroy_email(mut self, create_id: impl Into<String>) -> Self {
        self.arguments().on_success_destroy_email(create_id);
        self
    }

    #[must_use]
    pub fn on_success_destroy_email_id(mut self, id: impl Into<super::EmailSubmissionId>) -> Self {
        self.arguments().on_success_destroy_email_id(id);
        self
    }
}
