use crate::{
    Get,
    client::Client,
    core::{
        changes::ChangesResponse,
        query::{Comparator, Filter, QueryResponse},
    },
};

use super::{
    CalendarEventNotification, CalendarEventNotificationChanges, CalendarEventNotificationGet,
    CalendarEventNotificationQuery, CalendarEventNotificationSet, Property,
};

impl<Tr: crate::core::transport::HttpTransport> Client<Tr> {
    pub async fn calendar_event_notification_get(
        &self,
        id: &str,
        properties: Option<impl IntoIterator<Item = Property>>,
    ) -> crate::Result<Option<CalendarEventNotification>> {
        let mut request = self.build();
        let mut get = CalendarEventNotificationGet::new();
        get.ids([id]);
        if let Some(properties) = properties {
            get.properties(properties);
        }
        let handle = request.call(get)?;
        let mut response = request.send().await?;
        response.get(&handle).map(|mut r| r.take_list().pop())
    }

    pub async fn calendar_event_notification_destroy(&self, id: &str) -> crate::Result<()> {
        let mut request = self.build();
        let mut set = CalendarEventNotificationSet::new();
        set.destroy([id]);
        let handle = request.call(set)?;
        let mut response = request.send().await?;
        response.get(&handle)?.destroyed(id)
    }

    pub async fn calendar_event_notification_changes(
        &self,
        since_state: impl Into<String>,
        max_changes: std::num::NonZeroUsize,
    ) -> crate::Result<ChangesResponse<CalendarEventNotification<Get>>> {
        let mut request = self.build();
        let mut changes = CalendarEventNotificationChanges::new(since_state);
        changes.max_changes(max_changes);
        let handle = request.call(changes)?;
        let mut response = request.send().await?;
        response.get(&handle)
    }

    pub async fn calendar_event_notification_query(
        &self,
        filter: Option<impl Into<Filter<super::query::Filter>>>,
        sort: Option<impl IntoIterator<Item = Comparator<super::query::Comparator>>>,
    ) -> crate::Result<QueryResponse> {
        let mut request = self.build();
        let mut query = CalendarEventNotificationQuery::new();
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
}
