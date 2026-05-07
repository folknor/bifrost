use crate::{
    Get,
    client::Client,
    core::{changes::ChangesResponse, set::SetObject},
};

use super::{Identity, IdentityChanges, IdentityGet, IdentitySet, Property};

impl<Tr: crate::core::transport::HttpTransport> Client<Tr> {
    pub async fn identity_create(
        &self,
        name: impl Into<String>,
        email: impl Into<String>,
    ) -> crate::Result<Identity> {
        let mut request = self.build();
        let mut set = IdentitySet::new();
        let id = set.create().name(name).email(email).create_id().unwrap();
        let handle = request.call(set)?;
        let mut response = request.send().await?;
        response.get(&handle)?.created(&id)
    }

    pub async fn identity_destroy(&self, id: &str) -> crate::Result<()> {
        let mut request = self.build();
        let mut set = IdentitySet::new();
        set.destroy([id]);
        let handle = request.call(set)?;
        let mut response = request.send().await?;
        response.get(&handle)?.destroyed(id)
    }

    pub async fn identity_get(
        &self,
        id: &str,
        properties: Option<Vec<Property>>,
    ) -> crate::Result<Option<Identity>> {
        let mut request = self.build();
        let mut get = IdentityGet::new();
        get.ids([id]);
        if let Some(properties) = properties {
            get.properties(properties);
        }
        let handle = request.call(get)?;
        let mut response = request.send().await?;
        response.get(&handle).map(|r| r.into_list().pop())
    }

    pub async fn identity_changes(
        &self,
        since_state: impl Into<String>,
        max_changes: std::num::NonZeroUsize,
    ) -> crate::Result<ChangesResponse<Identity<Get>>> {
        let mut request = self.build();
        let mut changes = IdentityChanges::new(since_state);
        changes.max_changes(max_changes);
        let handle = request.call(changes)?;
        let mut response = request.send().await?;
        response.get(&handle)
    }
}
