use std::collections::HashMap;

use super::{ParticipantIdentity, ParticipantIdentityId};

impl ParticipantIdentity {
    pub(crate) fn id(&self) -> Option<&ParticipantIdentityId> {
        self.id.as_ref()
    }

    pub(crate) fn take_id(&mut self) -> ParticipantIdentityId {
        self.id
            .take()
            .unwrap_or_else(|| ParticipantIdentityId::new(""))
    }

    pub(crate) fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }

    pub(crate) fn send_to(&self) -> Option<&HashMap<String, String>> {
        self.send_to.as_ref()
    }

    pub(crate) fn is_default(&self) -> Option<bool> {
        self.is_default
    }
}
