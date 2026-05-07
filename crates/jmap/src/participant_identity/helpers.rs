use crate::{Get, client::Client, core::changes::ChangesResponse};

use super::{ParticipantIdentity, ParticipantIdentityChanges, ParticipantIdentityGet, Property};

impl<Tr: crate::core::transport::HttpTransport> Client<Tr> {
    pub async fn participant_identity_get(
        &self,
        id: &str,
        properties: Option<Vec<Property>>,
    ) -> crate::Result<Option<ParticipantIdentity>> {
        let mut request = self.build();
        let mut get = ParticipantIdentityGet::new();
        get.ids([id]);
        if let Some(properties) = properties {
            get.properties(properties);
        }
        let handle = request.call(get)?;
        let mut response = request.send().await?;
        response.get(&handle).map(|r| r.into_list().pop())
    }

    pub async fn participant_identity_changes(
        &self,
        since_state: impl Into<String>,
        max_changes: std::num::NonZeroUsize,
    ) -> crate::Result<ChangesResponse<ParticipantIdentity<Get>>> {
        let mut request = self.build();
        let mut changes = ParticipantIdentityChanges::new(since_state);
        changes.max_changes(max_changes);
        let handle = request.call(changes)?;
        let mut response = request.send().await?;
        response.get(&handle)
    }
}
