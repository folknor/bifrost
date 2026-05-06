use std::collections::HashMap;

use crate::{core::set::{SetObject, SetObjectCreatable}, Get, Set};

use super::ParticipantIdentity;

impl ParticipantIdentity<Set> {
    pub fn name(&mut self, name: impl Into<String>) -> &mut Self {
        self.name = Some(name.into());
        self
    }

    pub fn send_to(&mut self, send_to: HashMap<String, String>) -> &mut Self {
        self.send_to = Some(send_to);
        self
    }

    pub fn is_default(&mut self, is_default: bool) -> &mut Self {
        self.is_default = Some(is_default);
        self
    }
}

impl SetObject for ParticipantIdentity<Set> {
    type SetArguments = super::ParticipantIdentitySetArguments;

    fn create_id(&self) -> Option<String> {
        self._create_id.map(|id| format!("c{id}"))
    }
}

impl SetObjectCreatable for ParticipantIdentity<Set> {
    fn new(_create_id: Option<usize>) -> Self {
        ParticipantIdentity {
            _create_id,
            _state: Default::default(),
            id: None,
            name: None,
            send_to: None,
            is_default: None,
        }
    }
}

impl SetObject for ParticipantIdentity<Get> {
    type SetArguments = super::ParticipantIdentitySetArguments;

    fn create_id(&self) -> Option<String> {
        None
    }
}
