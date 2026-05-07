use crate::{
    client::Client,
    core::{
        query::{Comparator, Filter, QueryResponse},
        set::SetObject,
    },
};

use super::{
    Property, SieveScript, SieveScriptGet, SieveScriptQuery, SieveScriptSet,
    validate::SieveScriptValidateRequest,
};

impl<Tr: crate::core::transport::HttpTransport> Client<Tr> {
    pub async fn sieve_script_create(
        &self,
        name: impl Into<String>,
        script: impl Into<Vec<u8>>,
        activate: bool,
    ) -> crate::Result<SieveScript> {
        let blob_id = self
            .upload_to(&self.default_account(), script.into(), None)
            .await?
            .into_blob_id();
        let mut request = self.build();
        let mut set = SieveScriptSet::new();
        let id = set
            .create()
            .name(name)
            .blob_id(blob_id)
            .create_id()
            .unwrap();
        if activate {
            set.arguments().on_success_activate_script(id.clone());
        }
        let handle = request.call(set)?;
        let mut response = request.send().await?;
        response.get(&handle)?.created(&id)
    }

    pub async fn sieve_script_replace(
        &self,
        id: &str,
        script: impl Into<Vec<u8>>,
        activate: bool,
    ) -> crate::Result<Option<SieveScript>> {
        let blob_id = self
            .upload_to(&self.default_account(), script.into(), None)
            .await?
            .into_blob_id();
        let mut request = self.build();
        let mut set = SieveScriptSet::new();
        set.update(id).blob_id(blob_id);
        if activate {
            set.arguments().on_success_activate_script_id(id);
        }
        let handle = request.call(set)?;
        let mut response = request.send().await?;
        response.get(&handle)?.updated(id)
    }

    pub async fn sieve_script_rename(
        &self,
        id: &str,
        name: impl Into<String>,
        activate: bool,
    ) -> crate::Result<Option<SieveScript>> {
        let mut request = self.build();
        let mut set = SieveScriptSet::new();
        set.update(id).name(name);
        if activate {
            set.arguments().on_success_activate_script_id(id);
        }
        let handle = request.call(set)?;
        let mut response = request.send().await?;
        response.get(&handle)?.updated(id)
    }

    pub async fn sieve_script_activate(&self, id: &str) -> crate::Result<()> {
        let mut request = self.build();
        let mut set = SieveScriptSet::new();
        set.arguments().on_success_activate_script_id(id);
        let handle = request.call(set)?;
        let mut response = request.send().await?;
        response.get(&handle)?.unwrap_update_errors()
    }

    pub async fn sieve_script_deactivate(&self) -> crate::Result<()> {
        let mut request = self.build();
        let mut set = SieveScriptSet::new();
        set.arguments().on_success_deactivate_script(true);
        let handle = request.call(set)?;
        let mut response = request.send().await?;
        response.get(&handle)?.unwrap_update_errors()
    }

    pub async fn sieve_script_destroy(&self, id: &str) -> crate::Result<()> {
        let mut request = self.build();
        let mut set = SieveScriptSet::new();
        set.destroy([id]);
        let handle = request.call(set)?;
        let mut response = request.send().await?;
        response.get(&handle)?.destroyed(id)
    }

    pub async fn sieve_script_get(
        &self,
        id: &str,
        properties: Option<impl IntoIterator<Item = Property>>,
    ) -> crate::Result<Option<SieveScript>> {
        let mut request = self.build();
        let mut get = SieveScriptGet::new();
        get.ids([id]);
        if let Some(properties) = properties {
            get.properties(properties);
        }
        let handle = request.call(get)?;
        let mut response = request.send().await?;
        response.get(&handle).map(|r| r.into_list().pop())
    }

    pub async fn sieve_script_query(
        &self,
        filter: Option<impl Into<Filter<super::query::Filter>>>,
        sort: Option<impl IntoIterator<Item = Comparator<super::query::Comparator>>>,
    ) -> crate::Result<QueryResponse> {
        let mut request = self.build();
        let mut query = SieveScriptQuery::new();
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

    pub async fn sieve_script_validate(&self, script: impl Into<Vec<u8>>) -> crate::Result<()> {
        let blob_id = self
            .upload_to(&self.default_account(), script.into(), None)
            .await?
            .into_blob_id();
        let mut request = self.build();
        let validate = SieveScriptValidateRequest::new(blob_id);
        let handle = request.call(validate)?;
        let mut response = request.send().await?;
        response.get(&handle)?.unwrap_error()
    }
}
