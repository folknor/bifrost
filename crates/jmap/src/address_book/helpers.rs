use crate::{
    Get,
    client::Client,
    core::{changes::ChangesResponse, set::SetObject},
};

use super::{AddressBook, AddressBookChanges, AddressBookGet, AddressBookSet, Property};

impl<Tr: crate::core::transport::HttpTransport> Client<Tr> {
    pub async fn address_book_create(&self, name: impl Into<String>) -> crate::Result<AddressBook> {
        let mut request = self.build();
        let mut set = AddressBookSet::new();
        let id = set
            .create()
            .name(name)
            .is_subscribed(true)
            .create_id()
            .unwrap();
        let handle = request.call(set)?;
        let mut response = request.send().await?;
        response.get(&handle)?.created(&id)
    }

    pub async fn address_book_destroy(&self, id: &str, remove_contents: bool) -> crate::Result<()> {
        let mut request = self.build();
        let mut set = AddressBookSet::new();
        set.destroy([id])
            .arguments()
            .on_destroy_remove_contents(remove_contents);
        let handle = request.call(set)?;
        let mut response = request.send().await?;
        response.get(&handle)?.destroyed(id)
    }

    pub async fn address_book_get(
        &self,
        id: &str,
        properties: Option<impl IntoIterator<Item = Property>>,
    ) -> crate::Result<Option<AddressBook>> {
        let mut request = self.build();
        let mut get = AddressBookGet::new();
        get.ids([id]);
        if let Some(properties) = properties {
            get.properties(properties);
        }
        let handle = request.call(get)?;
        let mut response = request.send().await?;
        response.get(&handle).map(|mut r| r.take_list().pop())
    }

    pub async fn address_book_changes(
        &self,
        since_state: impl Into<String>,
        max_changes: std::num::NonZeroUsize,
    ) -> crate::Result<ChangesResponse<AddressBook<Get>>> {
        let mut request = self.build();
        let mut changes = AddressBookChanges::new(since_state);
        changes.max_changes(max_changes);
        let handle = request.call(changes)?;
        let mut response = request.send().await?;
        response.get(&handle)
    }
}
