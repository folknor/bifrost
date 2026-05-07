use crate::{
    Get,
    client::Client,
    core::{
        changes::ChangesResponse,
        query::{Comparator, Filter, QueryResponse},
        set::SetObject,
    },
    principal::ACL,
};

use super::{Mailbox, MailboxChanges, MailboxGet, MailboxQuery, MailboxSet, Property, Role};

impl<Tr: crate::core::transport::HttpTransport> Client<Tr> {
    pub async fn mailbox_create(
        &self,
        name: impl Into<String>,
        parent_id: Option<impl Into<String>>,
        role: Role,
    ) -> crate::Result<Mailbox> {
        let mut request = self.build();
        let mut set = MailboxSet::new();
        let id = set
            .create()
            .name(name)
            .role(role)
            .parent_id(parent_id)
            .create_id()
            .unwrap();
        let handle = request.call(set)?;
        let mut response = request.send().await?;
        response.get(&handle)?.created(&id)
    }

    pub async fn mailbox_rename(
        &self,
        id: &str,
        name: impl Into<String>,
    ) -> crate::Result<Option<Mailbox>> {
        let mut request = self.build();
        let mut set = MailboxSet::new();
        set.update(id).name(name);
        let handle = request.call(set)?;
        let mut response = request.send().await?;
        response.get(&handle)?.updated(id)
    }

    pub async fn mailbox_move(
        &self,
        id: &str,
        parent_id: Option<impl Into<String>>,
    ) -> crate::Result<Option<Mailbox>> {
        let mut request = self.build();
        let mut set = MailboxSet::new();
        set.update(id).parent_id(parent_id);
        let handle = request.call(set)?;
        let mut response = request.send().await?;
        response.get(&handle)?.updated(id)
    }

    pub async fn mailbox_update_role(
        &self,
        id: &str,
        role: Role,
    ) -> crate::Result<Option<Mailbox>> {
        let mut request = self.build();
        let mut set = MailboxSet::new();
        set.update(id).role(role);
        let handle = request.call(set)?;
        let mut response = request.send().await?;
        response.get(&handle)?.updated(id)
    }

    pub async fn mailbox_update_acl(
        &self,
        id: &str,
        account_id: &str,
        acl: impl IntoIterator<Item = ACL>,
    ) -> crate::Result<Option<Mailbox>> {
        let mut request = self.build();
        let mut set = MailboxSet::new();
        set.update(id).acl(account_id, acl);
        let handle = request.call(set)?;
        let mut response = request.send().await?;
        response.get(&handle)?.updated(id)
    }

    pub async fn mailbox_update_sort_order(
        &self,
        id: &str,
        sort_order: u32,
    ) -> crate::Result<Option<Mailbox>> {
        let mut request = self.build();
        let mut set = MailboxSet::new();
        set.update(id).sort_order(sort_order);
        let handle = request.call(set)?;
        let mut response = request.send().await?;
        response.get(&handle)?.updated(id)
    }

    pub async fn mailbox_subscribe(
        &self,
        id: &str,
        is_subscribed: bool,
    ) -> crate::Result<Option<Mailbox>> {
        let mut request = self.build();
        let mut set = MailboxSet::new();
        set.update(id).is_subscribed(is_subscribed);
        let handle = request.call(set)?;
        let mut response = request.send().await?;
        response.get(&handle)?.updated(id)
    }

    pub async fn mailbox_destroy(&self, id: &str, delete_emails: bool) -> crate::Result<()> {
        let mut request = self.build();
        let mut set = MailboxSet::new();
        set.destroy([id])
            .arguments()
            .on_destroy_remove_emails(delete_emails);
        let handle = request.call(set)?;
        let mut response = request.send().await?;
        response.get(&handle)?.destroyed(id)
    }

    pub async fn mailbox_get(
        &self,
        id: &str,
        properties: Option<impl IntoIterator<Item = Property>>,
    ) -> crate::Result<Option<Mailbox>> {
        let mut request = self.build();
        let mut get = MailboxGet::new();
        get.ids([id]);
        if let Some(properties) = properties {
            get.properties(properties);
        }
        let handle = request.call(get)?;
        let mut response = request.send().await?;
        response.get(&handle).map(|mut r| r.take_list().pop())
    }

    pub async fn mailbox_query(
        &self,
        filter: Option<impl Into<Filter<super::query::Filter>>>,
        sort: Option<impl IntoIterator<Item = Comparator<super::query::Comparator>>>,
    ) -> crate::Result<QueryResponse> {
        let mut request = self.build();
        let mut query = MailboxQuery::new();
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

    pub async fn mailbox_changes(
        &self,
        since_state: impl Into<String>,
        max_changes: usize,
    ) -> crate::Result<ChangesResponse<Mailbox<Get>>> {
        let mut request = self.build();
        let mut changes = MailboxChanges::new(since_state);
        changes.max_changes(max_changes);
        let handle = request.call(changes)?;
        let mut response = request.send().await?;
        response.get(&handle)
    }
}
