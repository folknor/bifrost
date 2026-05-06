use crate::client::Client;

use super::{
    copy::CopyBlobRequest,
    manage::BlobUploadRequest,
};

impl<Tr: crate::core::transport::HttpTransport> Client<Tr> {
    pub async fn blob_copy(
        &self,
        from_account_id: impl Into<String>,
        blob_id: impl Into<String>,
    ) -> crate::Result<String> {
        let blob_id = blob_id.into();
        let mut request = self.build();
        let account_id = request.default_account_id().to_string();
        let mut copy = CopyBlobRequest::new(&account_id, from_account_id);
        copy.blob_id(&blob_id);
        let handle = request.call(copy)?;
        let mut response = request.send().await?;
        response.get(&handle)?.copied(&blob_id)
    }

    pub async fn blob_upload_text(
        &self,
        text: impl Into<String>,
        type_: Option<impl Into<String>>,
    ) -> crate::Result<String> {
        let mut request = self.build();
        let account_id = request.default_account_id().to_string();
        let mut upload = BlobUploadRequest::new(&account_id);
        let id = upload.create_from_text(text, type_);
        let handle = request.call(upload)?;
        let mut response = request.send().await?;
        response.get(&handle)?.created(&id).map(|c| c.id)
    }
}
