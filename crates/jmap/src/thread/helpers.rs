use crate::client::Client;

use super::{Thread, ThreadGet};

impl<Tr: crate::core::transport::HttpTransport> Client<Tr> {
    pub async fn thread_get(&self, id: &str) -> crate::Result<Option<Thread>> {
        let mut request = self.build();
        let account_id = request.default_account_id().to_string();
        let mut get = ThreadGet::new(&account_id);
        get.ids([id]);
        let handle = request.call(get)?;
        let mut response = request.send().await?;
        response.get(&handle).map(|mut r| r.take_list().pop())
    }
}
