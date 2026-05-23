use super::{ACL, DKIM, Principal, PrincipalAccount, PrincipalId, Type};
use crate::core::id::AccountId;
use std::collections::HashMap;

impl Principal {
    pub(crate) fn id(&self) -> Option<&PrincipalId> {
        self.id.as_ref()
    }

    pub(crate) fn take_id(&mut self) -> PrincipalId {
        self.id.take().unwrap_or_else(|| PrincipalId::new(""))
    }

    pub(crate) fn ptype(&self) -> Option<&Type> {
        self.ptype.as_ref()
    }

    pub(crate) fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }

    pub(crate) fn email(&self) -> Option<&str> {
        self.email.as_deref()
    }

    pub(crate) fn description(&self) -> Option<&str> {
        self.description.as_deref()
    }

    pub(crate) fn timezone(&self) -> Option<&str> {
        self.timezone.as_deref()
    }

    pub(crate) fn secret(&self) -> Option<&str> {
        self.secret.as_deref()
    }

    pub(crate) fn picture(&self) -> Option<&str> {
        self.picture.as_deref()
    }

    pub(crate) fn quota(&self) -> Option<u32> {
        self.quota
    }

    pub(crate) fn capabilities(&self) -> Option<&HashMap<String, serde_json::Value>> {
        self.capabilities.as_ref()
    }

    pub(crate) fn accounts(&self) -> Option<&HashMap<AccountId, PrincipalAccount>> {
        self.accounts.as_ref()
    }

    pub(crate) fn aliases(&self) -> Option<&[String]> {
        self.aliases.as_deref()
    }

    pub(crate) fn members(&self) -> Option<&[PrincipalId]> {
        self.members.as_deref()
    }

    pub(crate) fn dkim(&self) -> Option<&DKIM> {
        self.dkim.as_ref()
    }

    pub(crate) fn acl(&self) -> Option<&HashMap<PrincipalId, Vec<ACL>>> {
        self.acl.as_ref()
    }
}
