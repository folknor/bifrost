use crate::{
    Get,
    client::Client,
    core::{
        changes::ChangesResponse,
        query::{Comparator, Filter, QueryResponse},
        set::SetObject,
    },
};

use super::{
    DKIM, Principal, PrincipalChanges, PrincipalGet, PrincipalQuery, PrincipalSet, Property, Type,
};

impl<Tr: crate::core::transport::HttpTransport> Client<Tr> {
    pub async fn individual_create(
        &self,
        email: impl Into<String>,
        secret: impl Into<String>,
        name: impl Into<String>,
    ) -> crate::Result<Principal> {
        let mut request = self.build();
        let account_id = request.default_account_id().to_string();
        let mut set = PrincipalSet::new(&account_id);
        let id = set
            .create()
            .name(name)
            .secret(secret)
            .email(email)
            .ptype(Type::Individual)
            .create_id()
            .unwrap();
        let handle = request.call(set)?;
        let mut response = request.send().await?;
        response.get(&handle)?.created(&id)
    }

    pub async fn domain_create(&self, name: impl Into<String>) -> crate::Result<Principal> {
        let mut request = self.build();
        let account_id = request.default_account_id().to_string();
        let mut set = PrincipalSet::new(&account_id);
        let id = set
            .create()
            .name(name)
            .ptype(Type::Domain)
            .create_id()
            .unwrap();
        let handle = request.call(set)?;
        let mut response = request.send().await?;
        response.get(&handle)?.created(&id)
    }

    pub async fn domain_enable_dkim(
        &self,
        id: &str,
        key: impl Into<String>,
        selector: impl Into<String>,
        expiration: Option<i64>,
    ) -> crate::Result<Option<Principal>> {
        let mut request = self.build();
        let account_id = request.default_account_id().to_string();
        let mut set = PrincipalSet::new(&account_id);
        set.update(id)
            .secret(key)
            .dkim(DKIM::new(Some(selector), expiration));
        let handle = request.call(set)?;
        let mut response = request.send().await?;
        response.get(&handle)?.updated(id)
    }

    pub async fn list_create(
        &self,
        email: impl Into<String>,
        name: impl Into<String>,
        members: impl IntoIterator<Item = impl Into<String>>,
    ) -> crate::Result<Principal> {
        let mut request = self.build();
        let account_id = request.default_account_id().to_string();
        let mut set = PrincipalSet::new(&account_id);
        let id = set
            .create()
            .name(name)
            .email(email)
            .ptype(Type::List)
            .members(members.into())
            .create_id()
            .unwrap();
        let handle = request.call(set)?;
        let mut response = request.send().await?;
        response.get(&handle)?.created(&id)
    }

    pub async fn group_create(
        &self,
        email: impl Into<String>,
        name: impl Into<String>,
        members: impl IntoIterator<Item = impl Into<String>>,
    ) -> crate::Result<Principal> {
        let mut request = self.build();
        let account_id = request.default_account_id().to_string();
        let mut set = PrincipalSet::new(&account_id);
        let id = set
            .create()
            .name(name)
            .email(email)
            .ptype(Type::Group)
            .members(members.into())
            .create_id()
            .unwrap();
        let handle = request.call(set)?;
        let mut response = request.send().await?;
        response.get(&handle)?.created(&id)
    }

    pub async fn principal_set_name(
        &self,
        id: &str,
        name: impl Into<String>,
    ) -> crate::Result<Option<Principal>> {
        let mut request = self.build();
        let account_id = request.default_account_id().to_string();
        let mut set = PrincipalSet::new(&account_id);
        set.update(id).name(name);
        let handle = request.call(set)?;
        let mut response = request.send().await?;
        response.get(&handle)?.updated(id)
    }

    pub async fn principal_set_secret(
        &self,
        id: &str,
        secret: impl Into<String>,
    ) -> crate::Result<Option<Principal>> {
        let mut request = self.build();
        let account_id = request.default_account_id().to_string();
        let mut set = PrincipalSet::new(&account_id);
        set.update(id).secret(secret);
        let handle = request.call(set)?;
        let mut response = request.send().await?;
        response.get(&handle)?.updated(id)
    }

    pub async fn principal_set_email(
        &self,
        id: &str,
        email: impl Into<String>,
    ) -> crate::Result<Option<Principal>> {
        let mut request = self.build();
        let account_id = request.default_account_id().to_string();
        let mut set = PrincipalSet::new(&account_id);
        set.update(id).email(email);
        let handle = request.call(set)?;
        let mut response = request.send().await?;
        response.get(&handle)?.updated(id)
    }

    pub async fn principal_set_timezone(
        &self,
        id: &str,
        timezone: Option<impl Into<String>>,
    ) -> crate::Result<Option<Principal>> {
        let mut request = self.build();
        let account_id = request.default_account_id().to_string();
        let mut set = PrincipalSet::new(&account_id);
        set.update(id).timezone(timezone);
        let handle = request.call(set)?;
        let mut response = request.send().await?;
        response.get(&handle)?.updated(id)
    }

    pub async fn principal_set_members(
        &self,
        id: &str,
        members: Option<impl IntoIterator<Item = impl Into<String>>>,
    ) -> crate::Result<Option<Principal>> {
        let mut request = self.build();
        let account_id = request.default_account_id().to_string();
        let mut set = PrincipalSet::new(&account_id);
        set.update(id).members(members);
        let handle = request.call(set)?;
        let mut response = request.send().await?;
        response.get(&handle)?.updated(id)
    }

    pub async fn principal_set_aliases(
        &self,
        id: &str,
        aliases: Option<impl IntoIterator<Item = impl Into<String>>>,
    ) -> crate::Result<Option<Principal>> {
        let mut request = self.build();
        let account_id = request.default_account_id().to_string();
        let mut set = PrincipalSet::new(&account_id);
        set.update(id).aliases(aliases);
        let handle = request.call(set)?;
        let mut response = request.send().await?;
        response.get(&handle)?.updated(id)
    }

    /// Update a principal's capabilities map (RFC 9670).
    pub async fn principal_set_capabilities(
        &self,
        id: &str,
        capabilities: Option<std::collections::HashMap<String, serde_json::Value>>,
    ) -> crate::Result<Option<Principal>> {
        let mut request = self.build();
        let account_id = request.default_account_id().to_string();
        let mut set = PrincipalSet::new(&account_id);
        set.update(id).capabilities(capabilities);
        let handle = request.call(set)?;
        let mut response = request.send().await?;
        response.get(&handle)?.updated(id)
    }

    pub async fn principal_destroy(&self, id: &str) -> crate::Result<()> {
        let mut request = self.build();
        let account_id = request.default_account_id().to_string();
        let mut set = PrincipalSet::new(&account_id);
        set.destroy([id]).arguments();
        let handle = request.call(set)?;
        let mut response = request.send().await?;
        response.get(&handle)?.destroyed(id)
    }

    pub async fn principal_get(
        &self,
        id: &str,
        properties: Option<impl IntoIterator<Item = Property>>,
    ) -> crate::Result<Option<Principal>> {
        let mut request = self.build();
        let account_id = request.default_account_id().to_string();
        let mut get = PrincipalGet::new(&account_id);
        get.ids([id]);
        if let Some(properties) = properties {
            get.properties(properties);
        }
        let handle = request.call(get)?;
        let mut response = request.send().await?;
        response.get(&handle).map(|mut r| r.take_list().pop())
    }

    pub async fn principal_query(
        &self,
        filter: Option<impl Into<Filter<super::query::Filter>>>,
        sort: Option<impl IntoIterator<Item = Comparator<super::query::Comparator>>>,
    ) -> crate::Result<QueryResponse> {
        let mut request = self.build();
        let account_id = request.default_account_id().to_string();
        let mut query = PrincipalQuery::new(&account_id);
        if let Some(filter) = filter {
            query.filter(filter);
        }
        if let Some(sort) = sort {
            query.sort(sort);
        }
        let handle = request.call(query)?;
        let mut response = request.send().await?;
        response.get(&handle)
    }

    pub async fn principal_changes(
        &self,
        since_state: impl Into<String>,
        max_changes: usize,
    ) -> crate::Result<ChangesResponse<Principal<Get>>> {
        let mut request = self.build();
        let account_id = request.default_account_id().to_string();
        let mut changes = PrincipalChanges::new(&account_id, since_state);
        changes.max_changes(max_changes);
        let handle = request.call(changes)?;
        let mut response = request.send().await?;
        response.get(&handle)
    }
}
