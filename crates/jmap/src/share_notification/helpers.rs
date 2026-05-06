use crate::{
    client::Client,
    core::{
        changes::ChangesResponse,
        query::{Comparator, Filter, QueryResponse},
    },
    Get,
};

use super::{
    Property, ShareNotification, ShareNotificationChanges, ShareNotificationGet,
    ShareNotificationQuery, ShareNotificationQueryChanges, ShareNotificationSet,
};

impl<Tr: crate::core::transport::HttpTransport> Client<Tr> {
    pub async fn share_notification_get(
        &self,
        id: &str,
        properties: Option<impl IntoIterator<Item = Property>>,
    ) -> crate::Result<Option<ShareNotification>> {
        let mut request = self.build();
        let account_id = request.default_account_id().to_string();
        let mut get = ShareNotificationGet::new(&account_id);
        get.ids([id]);
        if let Some(properties) = properties {
            get.properties(properties);
        }
        let handle = request.call(get)?;
        let mut response = request.send().await?;
        response.get(&handle).map(|mut r| r.take_list().pop())
    }

    /// Destroy a share notification. This is the only mutation permitted
    /// on ShareNotification objects (no create or update).
    pub async fn share_notification_destroy(&self, id: &str) -> crate::Result<()> {
        let mut request = self.build();
        let account_id = request.default_account_id().to_string();
        let mut set = ShareNotificationSet::new(&account_id);
        set.destroy([id]);
        let handle = request.call(set)?;
        let mut response = request.send().await?;
        response.get(&handle)?.destroyed(id)
    }

    pub async fn share_notification_changes(
        &self,
        since_state: impl Into<String>,
        max_changes: usize,
    ) -> crate::Result<ChangesResponse<ShareNotification<Get>>> {
        let mut request = self.build();
        let account_id = request.default_account_id().to_string();
        let mut changes = ShareNotificationChanges::new(&account_id, since_state);
        changes.max_changes(max_changes);
        let handle = request.call(changes)?;
        let mut response = request.send().await?;
        response.get(&handle)
    }

    pub async fn share_notification_query(
        &self,
        filter: Option<impl Into<Filter<super::query::Filter>>>,
        sort: Option<impl IntoIterator<Item = Comparator<super::query::Comparator>>>,
    ) -> crate::Result<QueryResponse> {
        let mut request = self.build();
        let account_id = request.default_account_id().to_string();
        let mut query = ShareNotificationQuery::new(&account_id);
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

    pub async fn share_notification_query_changes(
        &self,
        since_query_state: impl Into<String>,
        filter: Option<impl Into<Filter<super::query::Filter>>>,
        sort: Option<impl IntoIterator<Item = Comparator<super::query::Comparator>>>,
    ) -> crate::Result<crate::core::query_changes::QueryChangesResponse> {
        let mut request = self.build();
        let account_id = request.default_account_id().to_string();
        let mut qc = ShareNotificationQueryChanges::new(&account_id, since_query_state);
        if let Some(filter) = filter {
            qc.filter(filter);
        }
        if let Some(sort) = sort {
            qc.sort(sort);
        }
        let handle = request.call(qc)?;
        let mut response = request.send().await?;
        response.get(&handle)
    }
}
