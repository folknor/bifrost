use crate::{
    client::Client,
    core::{
        changes::ChangesResponse,
        query::{Comparator, Filter, QueryResponse},
        set::SetObject,
    },
    Get,
};

use super::{
    Address, EmailSubmission, EmailSubmissionChanges, EmailSubmissionGet, EmailSubmissionQuery, EmailSubmissionSet, Property, UndoStatus,
};

impl<Tr: crate::core::transport::HttpTransport> Client<Tr> {
    pub async fn email_submission_create(
        &self,
        email_id: impl Into<String>,
        identity_id: impl Into<String>,
    ) -> crate::Result<EmailSubmission<Get>> {
        let mut request = self.build();
        let account_id = request.default_account_id().to_string();
        request.add_capability::<crate::core::capability::Mail>();
        let mut set = EmailSubmissionSet::new(&account_id);
        let id = set
            .create()
            .email_id(email_id)
            .identity_id(identity_id)
            .create_id()
            .unwrap();
        let handle = request.call(set)?;
        let mut response = request.send().await?;
        response.get(&handle)?.created(&id)
    }

    pub async fn email_submission_create_envelope<S, T, U>(
        &self,
        email_id: impl Into<String>,
        identity_id: impl Into<String>,
        mail_from: S,
        rcpt_to: T,
    ) -> crate::Result<EmailSubmission<Get>>
    where
        S: Into<Address>,
        T: IntoIterator<Item = U>,
        U: Into<Address>,
    {
        let mut request = self.build();
        let account_id = request.default_account_id().to_string();
        request.add_capability::<crate::core::capability::Mail>();
        let mut set = EmailSubmissionSet::new(&account_id);
        let id = set
            .create()
            .email_id(email_id)
            .identity_id(identity_id)
            .envelope(mail_from, rcpt_to)
            .create_id()
            .unwrap();
        let handle = request.call(set)?;
        let mut response = request.send().await?;
        response.get(&handle)?.created(&id)
    }

    pub async fn email_submission_change_status(
        &self,
        id: &str,
        undo_status: UndoStatus,
    ) -> crate::Result<Option<EmailSubmission>> {
        let mut request = self.build();
        let account_id = request.default_account_id().to_string();
        request.add_capability::<crate::core::capability::Mail>();
        let mut set = EmailSubmissionSet::new(&account_id);
        set.update(id).undo_status(undo_status);
        let handle = request.call(set)?;
        let mut response = request.send().await?;
        response.get(&handle)?.updated(id)
    }

    pub async fn email_submission_destroy(&self, id: &str) -> crate::Result<()> {
        let mut request = self.build();
        let account_id = request.default_account_id().to_string();
        request.add_capability::<crate::core::capability::Mail>();
        let mut set = EmailSubmissionSet::new(&account_id);
        set.destroy([id]);
        let handle = request.call(set)?;
        let mut response = request.send().await?;
        response.get(&handle)?.destroyed(id)
    }

    pub async fn email_submission_get(
        &self,
        id: &str,
        properties: Option<Vec<Property>>,
    ) -> crate::Result<Option<EmailSubmission>> {
        let mut request = self.build();
        let account_id = request.default_account_id().to_string();
        request.add_capability::<crate::core::capability::Mail>();
        let mut get = EmailSubmissionGet::new(&account_id);
        get.ids([id]);
        if let Some(properties) = properties {
            get.properties(properties);
        }
        let handle = request.call(get)?;
        let mut response = request.send().await?;
        response.get(&handle).map(|mut r| r.take_list().pop())
    }

    pub async fn email_submission_query(
        &self,
        filter: Option<impl Into<Filter<super::query::Filter>>>,
        sort: Option<impl IntoIterator<Item = Comparator<super::query::Comparator>>>,
    ) -> crate::Result<QueryResponse> {
        let mut request = self.build();
        let account_id = request.default_account_id().to_string();
        request.add_capability::<crate::core::capability::Mail>();
        let mut query = EmailSubmissionQuery::new(&account_id);
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

    pub async fn email_submission_changes(
        &self,
        since_state: impl Into<String>,
        max_changes: usize,
    ) -> crate::Result<ChangesResponse<EmailSubmission<Get>>> {
        let mut request = self.build();
        let account_id = request.default_account_id().to_string();
        request.add_capability::<crate::core::capability::Mail>();
        let mut changes = EmailSubmissionChanges::new(&account_id, since_state);
        changes.max_changes(max_changes);
        let handle = request.call(changes)?;
        let mut response = request.send().await?;
        response.get(&handle)
    }
}
