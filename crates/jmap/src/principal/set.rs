use super::{ACL, DKIM, PrincipalAccount, PrincipalCreate, PrincipalPatch, Property, Type};
use std::collections::HashMap;

macro_rules! principal_setters {
    ($t:ty) => {
        impl $t {
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
                self.aliases =
                    aliases.map(|l| l.into_iter().map(std::convert::Into::into).collect());
                self
            }

            pub fn capabilities(
                &mut self,
                capabilities: Option<HashMap<String, serde_json::Value>>,
            ) -> &mut Self {
                self.capabilities = capabilities;
                self
            }

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
                self.members =
                    members.map(|l| l.into_iter().map(std::convert::Into::into).collect());
                self
            }
        }
    };
}

principal_setters!(PrincipalCreate);
principal_setters!(PrincipalPatch);

impl PrincipalPatch {
    pub fn alias(&mut self, alias: &str, set: bool) -> &mut Self {
        self.property_patch
            .get_or_insert_with(HashMap::new)
            .insert(format!("{}/{}", Property::Aliases, alias), set);
        self
    }

    pub fn member(&mut self, member: &str, set: bool) -> &mut Self {
        self.property_patch
            .get_or_insert_with(HashMap::new)
            .insert(format!("{}/{}", Property::Members, member), set);
        self
    }
}
