use crate::{
    Get,
    client::Client,
    core::{changes::ChangesResponse, set::SetObject},
};

use super::{Calendar, CalendarChanges, CalendarGet, CalendarSet, Property};

impl<Tr: crate::core::transport::HttpTransport> Client<Tr> {
    pub async fn calendar_create(&self, name: impl Into<String>) -> crate::Result<Calendar> {
        let mut request = self.build();
        let mut set = CalendarSet::new();
        let id = set
            .create()
            .name(name)
            .is_subscribed(true)
            .is_visible(true)
            .create_id()
            .unwrap();
        let handle = request.call(set)?;
        let mut response = request.send().await?;
        response.get(&handle)?.created(&id)
    }

    pub async fn calendar_destroy(&self, id: &str, remove_events: bool) -> crate::Result<()> {
        let mut request = self.build();
        let mut set = CalendarSet::new();
        set.destroy([id])
            .arguments()
            .on_destroy_remove_events(remove_events);
        let handle = request.call(set)?;
        let mut response = request.send().await?;
        response.get(&handle)?.destroyed(id)
    }

    pub async fn calendar_get(
        &self,
        id: &str,
        properties: Option<impl IntoIterator<Item = Property>>,
    ) -> crate::Result<Option<Calendar>> {
        let mut request = self.build();
        let mut get = CalendarGet::new();
        get.ids([id]);
        if let Some(properties) = properties {
            get.properties(properties);
        }
        let handle = request.call(get)?;
        let mut response = request.send().await?;
        response.get(&handle).map(|mut r| r.take_list().pop())
    }

    pub async fn calendar_changes(
        &self,
        since_state: impl Into<String>,
        max_changes: usize,
    ) -> crate::Result<ChangesResponse<Calendar<Get>>> {
        let mut request = self.build();
        let mut changes = CalendarChanges::new(since_state);
        changes.max_changes(max_changes);
        let handle = request.call(changes)?;
        let mut response = request.send().await?;
        response.get(&handle)
    }
}
