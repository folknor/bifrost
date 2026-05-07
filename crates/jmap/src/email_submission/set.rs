use super::{Address, EmailSubmission, Envelope, SetArguments, UndoStatus};
use crate::{
    Get, Set,
    core::set::{SetObject, SetObjectCreatable},
    email::Email,
};
use std::collections::HashMap;

impl EmailSubmission<Set> {
    pub fn identity_id(&mut self, identity_id: impl Into<String>) -> &mut Self {
        self.identity_id = Some(identity_id.into());
        self
    }

    pub fn email_id(&mut self, email_id: impl Into<String>) -> &mut Self {
        self.email_id = Some(email_id.into());
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

impl SetObject for EmailSubmission<Set> {
    type SetArguments = SetArguments;

    fn create_id(&self) -> Option<String> {
        self._create_id.map(|id| format!("c{id}"))
    }
}

impl SetObjectCreatable for EmailSubmission<Set> {
    fn new(_create_id: Option<usize>) -> Self {
        EmailSubmission {
            _create_id,
            _state: Default::default(),
            id: None,
            identity_id: None,
            email_id: None,
            thread_id: None,
            envelope: None,
            send_at: None,
            undo_status: None,
            delivery_status: None,
            dsn_blob_ids: None,
            mdn_blob_ids: None,
        }
    }
}

impl SetObject for EmailSubmission<Get> {
    type SetArguments = SetArguments;

    fn create_id(&self) -> Option<String> {
        None
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

impl Address<Set> {
    pub fn new(email: impl Into<String>) -> Address<Set> {
        Address {
            _state: Default::default(),
            email: email.into(),
            parameters: None,
        }
    }

    pub fn parameter(
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
            _state: Default::default(),
            email,
            parameters: None,
        }
    }
}

impl From<&str> for Address {
    fn from(email: &str) -> Self {
        Address {
            _state: Default::default(),
            email: email.to_string(),
            parameters: None,
        }
    }
}

impl From<Address<Set>> for Address<Get> {
    fn from(addr: Address<Set>) -> Self {
        Address {
            _state: Default::default(),
            email: addr.email,
            parameters: addr.parameters,
        }
    }
}

impl From<Address<Get>> for Address<Set> {
    fn from(addr: Address<Get>) -> Self {
        Address {
            _state: Default::default(),
            email: addr.email,
            parameters: addr.parameters,
        }
    }
}

impl SetArguments {
    pub fn on_success_update_email(&mut self, id: impl Into<String>) -> &mut Email<Set> {
        self.on_success_update_email_(format!("#{}", id.into()))
    }

    pub fn on_success_update_email_id(&mut self, id: impl Into<String>) -> &mut Email<Set> {
        self.on_success_update_email_(id)
    }

    fn on_success_update_email_(&mut self, id: impl Into<String>) -> &mut Email<Set> {
        let id = id.into();
        self.on_success_update_email
            .get_or_insert_with(HashMap::new)
            .insert(id.clone(), Email::new(None));
        self.on_success_update_email
            .as_mut()
            .unwrap()
            .get_mut(&id)
            .unwrap()
    }

    pub fn on_success_destroy_email(&mut self, id: impl Into<String>) -> &mut Self {
        self.on_success_destroy_email
            .get_or_insert_with(Vec::new)
            .push(format!("#{}", id.into()));
        self
    }

    pub fn on_success_destroy_email_id(&mut self, id: impl Into<String>) -> &mut Self {
        self.on_success_destroy_email
            .get_or_insert_with(Vec::new)
            .push(id.into());
        self
    }
}

// -- Lifted method arguments (plans/API.md §5) --

use super::EmailSubmissionSet;

impl EmailSubmissionSet {
    /// Stays imperative because the returned `&mut Email<Set>` writes
    /// into a HashMap entry inside the request's arguments.
    pub fn on_success_update_email(&mut self, id: impl Into<String>) -> &mut Email<Set> {
        self.arguments().on_success_update_email(id)
    }

    /// Stays imperative for the same reason as
    /// `on_success_update_email`.
    pub fn on_success_update_email_id(&mut self, id: impl Into<String>) -> &mut Email<Set> {
        self.arguments().on_success_update_email_id(id)
    }

    #[must_use]
    pub fn on_success_destroy_email(mut self, id: impl Into<String>) -> Self {
        self.arguments().on_success_destroy_email(id);
        self
    }

    #[must_use]
    pub fn on_success_destroy_email_id(mut self, id: impl Into<String>) -> Self {
        self.arguments().on_success_destroy_email_id(id);
        self
    }
}
