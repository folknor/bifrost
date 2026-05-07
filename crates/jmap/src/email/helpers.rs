use crate::{
    Get,
    client::Client,
    core::{
        changes::ChangesResponse,
        query::{Comparator, Filter, QueryResponse},
        query_changes::QueryChangesResponse,
    },
};

use super::{
    BodyProperty, Email, EmailChanges, EmailCopy, EmailGet, EmailQuery, EmailQueryChanges,
    EmailSet, Property,
    import::EmailImportRequest,
    parse::EmailParseRequest,
    search_snippet::{SearchSnippetGetRequest, SearchSnippetGetResponse},
};

impl<Tr: crate::core::transport::HttpTransport> Client<Tr> {
    pub async fn email_import<T, U, V, W>(
        &self,
        raw_message: Vec<u8>,
        mailbox_ids: T,
        keywords: Option<V>,
        received_at: Option<i64>,
    ) -> crate::Result<Email>
    where
        T: IntoIterator<Item = U>,
        U: Into<String>,
        V: IntoIterator<Item = W>,
        W: Into<String>,
    {
        self.email_import_account(
            self.default_account_id(),
            raw_message,
            mailbox_ids,
            keywords,
            received_at,
        )
        .await
    }

    pub async fn email_import_account<T, U, V, W>(
        &self,
        account_id: &str,
        raw_message: Vec<u8>,
        mailbox_ids: T,
        keywords: Option<V>,
        received_at: Option<i64>,
    ) -> crate::Result<Email>
    where
        T: IntoIterator<Item = U>,
        U: Into<String>,
        V: IntoIterator<Item = W>,
        W: Into<String>,
    {
        let blob_id = self
            .upload_to(
                &crate::core::id::AccountId::new(account_id),
                raw_message,
                None,
            )
            .await?
            .take_blob_id();
        let mut request = self.build().account_id(account_id.to_string());
        let mut import = EmailImportRequest::new();
        let import_item = import.email(blob_id).mailbox_ids(mailbox_ids);

        if let Some(keywords) = keywords {
            import_item.keywords(keywords);
        }

        if let Some(received_at) = received_at {
            import_item.received_at(received_at);
        }

        let id = import_item.create_id();
        let handle = request.call(import)?;
        let mut response = request.send().await?;
        response.get(&handle)?.created(&id)
    }

    pub async fn email_set_mailbox(
        &self,
        id: &str,
        mailbox_id: &str,
        set: bool,
    ) -> crate::Result<Option<Email>> {
        let mut request = self.build();
        let mut email_set = EmailSet::new();
        email_set.update(id).mailbox_id(mailbox_id, set);
        let handle = request.call(email_set)?;
        let mut response = request.send().await?;
        response.get(&handle)?.updated(id)
    }

    pub async fn email_set_mailboxes<T, U>(
        &self,
        id: &str,
        mailbox_ids: T,
    ) -> crate::Result<Option<Email>>
    where
        T: IntoIterator<Item = U>,
        U: Into<String>,
    {
        let mut request = self.build();
        let mut email_set = EmailSet::new();
        email_set.update(id).mailbox_ids(mailbox_ids);
        let handle = request.call(email_set)?;
        let mut response = request.send().await?;
        response.get(&handle)?.updated(id)
    }

    pub async fn email_set_keyword(
        &self,
        id: &str,
        keyword: &str,
        set: bool,
    ) -> crate::Result<Option<Email>> {
        let mut request = self.build();
        let mut email_set = EmailSet::new();
        email_set.update(id).keyword(keyword, set);
        let handle = request.call(email_set)?;
        let mut response = request.send().await?;
        response.get(&handle)?.updated(id)
    }

    pub async fn email_set_keywords<T, U>(
        &self,
        id: &str,
        keywords: T,
    ) -> crate::Result<Option<Email>>
    where
        T: IntoIterator<Item = U>,
        U: Into<String>,
    {
        let mut request = self.build();
        let mut email_set = EmailSet::new();
        email_set.update(id).keywords(keywords);
        let handle = request.call(email_set)?;
        let mut response = request.send().await?;
        response.get(&handle)?.updated(id)
    }

    pub async fn email_destroy(&self, id: &str) -> crate::Result<()> {
        let mut request = self.build();
        let mut email_set = EmailSet::new();
        email_set.destroy([id]);
        let handle = request.call(email_set)?;
        let mut response = request.send().await?;
        response.get(&handle)?.destroyed(id)
    }

    pub async fn email_get(
        &self,
        id: &str,
        properties: Option<impl IntoIterator<Item = Property>>,
    ) -> crate::Result<Option<Email<Get>>> {
        let mut request = self.build();
        let mut get = EmailGet::new();
        get.ids([id]);
        if let Some(properties) = properties {
            get.properties(properties);
        }
        let handle = request.call(get)?;
        let mut response = request.send().await?;
        response.get(&handle).map(|mut r| r.take_list().pop())
    }

    pub async fn email_changes(
        &self,
        since_state: impl Into<String>,
        max_changes: Option<std::num::NonZeroUsize>,
    ) -> crate::Result<ChangesResponse<Email<Get>>> {
        let mut request = self.build();
        let mut changes = EmailChanges::new(since_state);
        if let Some(max_changes) = max_changes {
            changes.max_changes(max_changes);
        }
        let handle = request.call(changes)?;
        let mut response = request.send().await?;
        response.get(&handle)
    }

    pub async fn email_query(
        &self,
        filter: Option<impl Into<Filter<super::query::Filter>>>,
        sort: Option<impl IntoIterator<Item = Comparator<super::query::Comparator>>>,
    ) -> crate::Result<QueryResponse> {
        let mut request = self.build();
        let mut query = EmailQuery::new();
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

    pub async fn email_query_changes(
        &self,
        since_query_state: impl Into<String>,
        filter: Option<impl Into<Filter<super::query::Filter>>>,
    ) -> crate::Result<QueryChangesResponse> {
        let mut request = self.build();
        let mut query = EmailQueryChanges::new(since_query_state);
        if let Some(filter) = filter {
            query.filter(filter);
        }
        let handle = request.call(query)?;
        let mut response = request.send().await?;
        response.get(&handle)
    }

    pub async fn email_parse(
        &self,
        blob_id: &str,
        properties: Option<impl IntoIterator<Item = Property>>,
        body_properties: Option<impl IntoIterator<Item = BodyProperty>>,
        max_body_value_bytes: Option<usize>,
    ) -> crate::Result<Email> {
        let mut request = self.build();
        let mut parse = EmailParseRequest::new();
        parse.blob_ids([blob_id]);
        if let Some(properties) = properties {
            parse.properties(properties);
        }

        if let Some(body_properties) = body_properties {
            parse.body_properties(body_properties);
        }

        if let Some(max_body_value_bytes) = max_body_value_bytes {
            parse
                .fetch_all_body_values(true)
                .max_body_value_bytes(max_body_value_bytes);
        }

        let handle = request.call(parse)?;
        let mut response = request.send().await?;
        response.get(&handle).and_then(|mut r| r.parsed(blob_id))
    }

    pub async fn email_copy<T, U, V, W>(
        &self,
        from_account_id: impl Into<String>,
        id: impl Into<String>,
        mailbox_ids: T,
        keywords: Option<V>,
        received_at: Option<i64>,
    ) -> crate::Result<Email>
    where
        T: IntoIterator<Item = U>,
        U: Into<String>,
        V: IntoIterator<Item = W>,
        W: Into<String>,
    {
        let id = id.into();
        let mut request = self.build();
        let mut copy = EmailCopy::new(from_account_id);
        let email = copy.create(id.clone()).mailbox_ids(mailbox_ids);

        if let Some(keywords) = keywords {
            email.keywords(keywords);
        }

        if let Some(received_at) = received_at {
            email.received_at(received_at);
        }

        let handle = request.call(copy)?;
        let mut response = request.send().await?;
        response.get(&handle)?.created(&id)
    }

    pub async fn search_snippet_get(
        &self,
        filter: Option<impl Into<Filter<super::query::Filter>>>,
        email_ids: impl IntoIterator<Item = impl Into<String>>,
    ) -> crate::Result<SearchSnippetGetResponse> {
        let mut request = self.build();
        let mut snippet = SearchSnippetGetRequest::new();
        if let Some(filter) = filter {
            snippet.filter(filter);
        }
        snippet.email_ids(email_ids);
        let handle = request.call(snippet)?;
        let mut response = request.send().await?;
        response.get(&handle)
    }
}
