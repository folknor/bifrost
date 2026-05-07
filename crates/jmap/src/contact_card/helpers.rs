use crate::{
    Get,
    client::Client,
    core::{
        changes::ChangesResponse,
        query::{Comparator, Filter, QueryResponse},
    },
};

use super::{
    ContactCard, ContactCardChanges, ContactCardGet, ContactCardQuery, ContactCardSet, Property,
    parse::ContactCardParseRequest,
};

impl<Tr: crate::core::transport::HttpTransport> Client<Tr> {
    pub async fn contact_card_get(
        &self,
        id: &str,
        properties: Option<impl IntoIterator<Item = Property>>,
    ) -> crate::Result<Option<ContactCard>> {
        let mut request = self.build();
        let mut get = ContactCardGet::new();
        get.ids([id]);
        if let Some(properties) = properties {
            get.properties(properties);
        }
        let handle = request.call(get)?;
        let mut response = request.send().await?;
        response.get(&handle).map(|r| r.into_list().pop())
    }

    pub async fn contact_card_destroy(&self, id: &str) -> crate::Result<()> {
        let mut request = self.build();
        let mut set = ContactCardSet::new();
        set.destroy([id]);
        let handle = request.call(set)?;
        let mut response = request.send().await?;
        response.get(&handle)?.destroyed(id)
    }

    pub async fn contact_card_changes(
        &self,
        since_state: impl Into<String>,
        max_changes: Option<std::num::NonZeroUsize>,
    ) -> crate::Result<ChangesResponse<ContactCard<Get>>> {
        let mut request = self.build();
        let mut changes = ContactCardChanges::new(since_state);
        if let Some(max_changes) = max_changes {
            changes.max_changes(max_changes);
        }
        let handle = request.call(changes)?;
        let mut response = request.send().await?;
        response.get(&handle)
    }

    pub async fn contact_card_query(
        &self,
        filter: Option<impl Into<Filter<super::query::Filter>>>,
        sort: Option<impl IntoIterator<Item = Comparator<super::query::Comparator>>>,
    ) -> crate::Result<QueryResponse> {
        let mut request = self.build();
        let mut query = ContactCardQuery::new();
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

    pub async fn contact_card_parse(
        &self,
        blob_id: &str,
        properties: Option<impl IntoIterator<Item = Property>>,
    ) -> crate::Result<Vec<ContactCard>> {
        let mut request = self.build();
        let mut parse = ContactCardParseRequest::new();
        parse.blob_ids([blob_id]);
        if let Some(properties) = properties {
            parse.properties(properties);
        }
        let handle = request.call(parse)?;
        let mut response = request.send().await?;
        response.get(&handle).and_then(|mut r| r.parsed(blob_id))
    }
}
