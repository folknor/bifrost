use super::{Principal, PrincipalAccount, Property, Type, ACL, DKIM};
use crate::{core::set::{SetObject, SetObjectCreatable}, Get, Set};
use std::collections::HashMap;

impl Principal<Set> {
    pub fn name(&mut self, name: impl Into<String>) -> &mut Self {
        self.name = name.into().into();
        self
    }

    pub fn description(&mut self, description: Option<impl Into<String>>) -> &mut Self {
        self.description = description.map(std::convert::Into::into);
        self
    }

    pub fn email(&mut self, email: impl Into<String>) -> &mut Self {
        self.email = email.into().into();
        self
    }

    pub fn secret(&mut self, secret: impl Into<String>) -> &mut Self {
        self.secret = secret.into().into();
        self
    }

    pub fn timezone(&mut self, timezone: Option<impl Into<String>>) -> &mut Self {
        self.timezone = timezone.map(std::convert::Into::into);
        self
    }

    pub fn picture(&mut self, picture: Option<impl Into<String>>) -> &mut Self {
        self.picture = picture.map(std::convert::Into::into);
        self
    }

    pub fn quota(&mut self, quota: Option<u32>) -> &mut Self {
        self.quota = quota;
        self
    }

    pub fn ptype(&mut self, ptype: Type) -> &mut Self {
        self.ptype = ptype.into();
        self
    }

    pub fn dkim(&mut self, dkim: DKIM) -> &mut Self {
        self.dkim = dkim.into();
        self
    }

    pub fn acl(&mut self, acl: Option<HashMap<String, Vec<ACL>>>) -> &mut Self {
        self.acl = acl;
        self
    }

    pub fn aliases<T, U>(&mut self, aliases: Option<T>) -> &mut Self
    where
        T: IntoIterator<Item = U>,
        U: Into<String>,
    {
        self.aliases = aliases.map(|l| l.into_iter().map(std::convert::Into::into).collect());
        self
    }

    pub fn alias(&mut self, alias: &str, set: bool) -> &mut Self {
        self.property_patch
            .get_or_insert_with(HashMap::new)
            .insert(format!("{}/{}", Property::Aliases, alias), set);
        self
    }

    /// RFC 9670: Set capabilities as a map of URI to domain-specific metadata.
    pub fn capabilities(
        &mut self,
        capabilities: Option<HashMap<String, serde_json::Value>>,
    ) -> &mut Self {
        self.capabilities = capabilities;
        self
    }

    /// RFC 9670: Set accounts accessible to this principal.
    pub fn accounts(
        &mut self,
        accounts: Option<HashMap<String, PrincipalAccount>>,
    ) -> &mut Self {
        self.accounts = accounts;
        self
    }

    pub fn members<T, U>(&mut self, members: Option<T>) -> &mut Self
    where
        T: IntoIterator<Item = U>,
        U: Into<String>,
    {
        self.members = members.map(|l| l.into_iter().map(std::convert::Into::into).collect());
        self
    }

    pub fn member(&mut self, member: &str, set: bool) -> &mut Self {
        self.property_patch
            .get_or_insert_with(HashMap::new)
            .insert(format!("{}/{}", Property::Members, member), set);
        self
    }
}

impl SetObject for Principal<Set> {
    type SetArguments = ();

    fn create_id(&self) -> Option<String> {
        self._create_id.map(|id| format!("c{id}"))
    }
}

impl SetObjectCreatable for Principal<Set> {
    fn new(_create_id: Option<usize>) -> Self {
        Principal {
            _create_id,
            _state: Default::default(),
            id: None,
            ptype: None,
            name: String::new().into(),
            description: String::new().into(),
            email: String::new().into(),
            timezone: String::new().into(),
            capabilities: None,
            accounts: None,
            aliases: Vec::with_capacity(0).into(),
            secret: String::new().into(),
            dkim: None,
            quota: None,
            picture: String::new().into(),
            members: Vec::with_capacity(0).into(),
            acl: HashMap::with_capacity(0).into(),
            property_patch: None,
        }
    }
}

impl SetObject for Principal<Get> {
    type SetArguments = ();

    fn create_id(&self) -> Option<String> {
        None
    }
}
