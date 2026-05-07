use super::{ACL, DKIM, Principal, PrincipalAccount, Type};
use std::collections::HashMap;

impl Principal {
    pub fn id(&self) -> Option<&str> {
        self.id.as_deref()
    }

    pub fn take_id(&mut self) -> String {
        self.id.take().unwrap_or_default()
    }

    pub fn ptype(&self) -> Option<&Type> {
        self.ptype.as_ref()
    }

    pub fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }

    pub fn email(&self) -> Option<&str> {
        self.email.as_deref()
    }

    pub fn description(&self) -> Option<&str> {
        self.description.as_deref()
    }

    pub fn timezone(&self) -> Option<&str> {
        self.timezone.as_deref()
    }

    pub fn secret(&self) -> Option<&str> {
        self.secret.as_deref()
    }

    pub fn picture(&self) -> Option<&str> {
        self.picture.as_deref()
    }

    pub fn quota(&self) -> Option<u32> {
        self.quota
    }

    pub fn capabilities(&self) -> Option<&HashMap<String, serde_json::Value>> {
        self.capabilities.as_ref()
    }

    pub fn accounts(&self) -> Option<&HashMap<String, PrincipalAccount>> {
        self.accounts.as_ref()
    }

    pub fn aliases(&self) -> Option<&[String]> {
        self.aliases.as_deref()
    }

    pub fn members(&self) -> Option<&[String]> {
        self.members.as_deref()
    }

    pub fn dkim(&self) -> Option<&DKIM> {
        self.dkim.as_ref()
    }

    pub fn acl(&self) -> Option<&HashMap<String, Vec<ACL>>> {
        self.acl.as_ref()
    }
}
