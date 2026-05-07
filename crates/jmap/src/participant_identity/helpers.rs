use crate::{Get, client::Client, core::changes::ChangesResponse};

use super::{ParticipantIdentity, ParticipantIdentityChanges, ParticipantIdentityGet, Property};

impl<Tr: crate::core::transport::HttpTransport> Client<Tr> {
    pub async fn participant_identity_get(
        &self,
        id: &str,
        properties: Option<Vec<Property>>,
    ) -> crate::Result<Option<ParticipantIdentity>> {
        let mut request = self.build();
        let account_id = request.default_account_id().to_string();
        let mut get = ParticipantIdentityGet::new(&account_id);
        get.ids([id]);
        if let Some(properties) = properties {
            get.properties(properties);
        }
        let handle = request.call(get)?;
        let mut response = request.send().await?;
        response.get(&handle).map(|mut r| r.take_list().pop())
    }

    pub async fn participant_identity_changes(
        &self,
        since_state: impl Into<String>,
        max_changes: usize,
    ) -> crate::Result<ChangesResponse<ParticipantIdentity<Get>>> {
        let mut request = self.build();
        let account_id = request.default_account_id().to_string();
        let mut changes = ParticipantIdentityChanges::new(&account_id, since_state);
        changes.max_changes(max_changes);
        let handle = request.call(changes)?;
        let mut response = request.send().await?;
        response.get(&handle)
    }
}
