use crate::{
    Get,
    client::Client,
    core::{
        changes::ChangesResponse,
        query::{Comparator, Filter, QueryResponse},
        query_changes::QueryChangesResponse,
    },
};

use super::{Property, Quota, QuotaChanges, QuotaGet, QuotaQuery, QuotaQueryChanges};

impl<Tr: crate::core::transport::HttpTransport> Client<Tr> {
    /// Fetch all quotas for the default account.
    pub async fn quota_get_all(&self) -> crate::Result<Vec<Quota>> {
        let mut request = self.build();
        let get = QuotaGet::new();
        let handle = request.call(get)?;
        let mut response = request.send().await?;
        response.get(&handle).map(|mut r| r.take_list())
    }

    pub async fn quota_get(
        &self,
        id: &str,
        properties: Option<impl IntoIterator<Item = Property>>,
    ) -> crate::Result<Option<Quota>> {
        let mut request = self.build();
        let mut get = QuotaGet::new();
        get.ids([id]);
        if let Some(properties) = properties {
            get.properties(properties);
        }
        let handle = request.call(get)?;
        let mut response = request.send().await?;
        response.get(&handle).map(|mut r| r.take_list().pop())
    }

    pub async fn quota_changes(
        &self,
        since_state: impl Into<String>,
        max_changes: std::num::NonZeroUsize,
    ) -> crate::Result<ChangesResponse<Quota<Get>>> {
        let mut request = self.build();
        let mut changes = QuotaChanges::new(since_state);
        changes.max_changes(max_changes);
        let handle = request.call(changes)?;
        let mut response = request.send().await?;
        response.get(&handle)
    }

    pub async fn quota_query(
        &self,
        filter: Option<impl Into<Filter<super::query::Filter>>>,
        sort: Option<impl IntoIterator<Item = Comparator<super::query::Comparator>>>,
    ) -> crate::Result<QueryResponse> {
        let mut request = self.build();
        let mut query = QuotaQuery::new();
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

    pub async fn quota_query_changes(
        &self,
        since_query_state: impl Into<String>,
        filter: Option<impl Into<Filter<super::query::Filter>>>,
    ) -> crate::Result<QueryChangesResponse> {
        let mut request = self.build();
        let mut query = QuotaQueryChanges::new(since_query_state);
        if let Some(filter) = filter {
            query.filter(filter);
        }
        let handle = request.call(query)?;
        let mut response = request.send().await?;
        response.get(&handle)
    }
}
