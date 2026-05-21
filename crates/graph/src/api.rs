use crate::client::GraphClient;
use crate::types::{
    CONTACT_SELECT, GraphContact, GraphContactFolder, GraphCreateFolderRequest, GraphMailFolder,
    GraphMessage, GraphMessagePatch, GraphMoveRequest, GraphOutlookCategory, GraphProfile,
    GraphRenameFolderRequest, ODataCollection,
};

impl GraphClient {
    pub async fn get_profile(&self) -> Result<GraphProfile, String> {
        let prefix = self.api_path_prefix();
        self.get_json(&format!(
            "{prefix}?$select=displayName,mail,userPrincipalName"
        ))
        .await
    }

    pub async fn list_mail_folders(&self) -> Result<Vec<GraphMailFolder>, String> {
        let prefix = self.api_path_prefix();
        let mut folders = Vec::new();
        let mut next_url = Some(format!(
            "{prefix}/mailFolders?$select=id,displayName,parentFolderId,childFolderCount&$top=100"
        ));

        while let Some(url) = next_url {
            let page: ODataCollection<GraphMailFolder> = if url.starts_with("http") {
                self.get_absolute(&url).await?
            } else {
                self.get_json(&url).await?
            };
            folders.extend(page.value);
            next_url = page.next_link;
        }

        Ok(folders)
    }

    pub async fn list_mail_folders_recursive(&self) -> Result<Vec<GraphMailFolder>, String> {
        let prefix = self.api_path_prefix();
        let mut folders = self.list_mail_folders().await?;
        let mut queue: std::collections::VecDeque<String> = folders
            .iter()
            .filter(|folder| folder.child_folder_count.unwrap_or(0) > 0)
            .map(|folder| folder.id.clone())
            .collect();

        while let Some(parent_id) = queue.pop_front() {
            let enc_parent_id = urlencoding::encode(&parent_id);
            let mut next_url = Some(format!(
                "{prefix}/mailFolders/{enc_parent_id}/childFolders?$select=id,displayName,parentFolderId,childFolderCount&$top=100"
            ));

            while let Some(url) = next_url {
                let page: ODataCollection<GraphMailFolder> = if url.starts_with("http") {
                    self.get_absolute(&url).await?
                } else {
                    self.get_json(&url).await?
                };
                for folder in page.value {
                    if folder.child_folder_count.unwrap_or(0) > 0 {
                        queue.push_back(folder.id.clone());
                    }
                    folders.push(folder);
                }
                next_url = page.next_link;
            }
        }

        Ok(folders)
    }

    pub async fn get_mail_folder(&self, folder_id: &str) -> Result<GraphMailFolder, String> {
        let prefix = self.api_path_prefix();
        let enc_folder_id = urlencoding::encode(folder_id);
        self.get_json(&format!(
            "{prefix}/mailFolders/{enc_folder_id}?$select=id,displayName,parentFolderId,childFolderCount"
        ))
        .await
    }

    pub async fn create_mail_folder(
        &self,
        parent_folder_id: Option<&str>,
        display_name: &str,
    ) -> Result<GraphMailFolder, String> {
        let prefix = self.api_path_prefix();
        let body = GraphCreateFolderRequest {
            display_name: display_name.to_string(),
        };
        let path = match parent_folder_id {
            Some(parent) => {
                let enc_parent = urlencoding::encode(parent);
                format!("{prefix}/mailFolders/{enc_parent}/childFolders")
            }
            None => format!("{prefix}/mailFolders"),
        };
        self.post(&path, &body).await
    }

    pub async fn rename_mail_folder(
        &self,
        folder_id: &str,
        display_name: &str,
    ) -> Result<(), String> {
        let prefix = self.api_path_prefix();
        let enc_folder_id = urlencoding::encode(folder_id);
        let body = GraphRenameFolderRequest {
            display_name: display_name.to_string(),
        };
        self.patch(&format!("{prefix}/mailFolders/{enc_folder_id}"), &body)
            .await
    }

    pub async fn delete_mail_folder(&self, folder_id: &str) -> Result<(), String> {
        let prefix = self.api_path_prefix();
        let enc_folder_id = urlencoding::encode(folder_id);
        self.delete(&format!("{prefix}/mailFolders/{enc_folder_id}"))
            .await
    }

    pub async fn get_message(&self, message_id: &str) -> Result<GraphMessage, String> {
        let prefix = self.api_path_prefix();
        let enc_message_id = urlencoding::encode(message_id);
        self.get_json(&format!("{prefix}/messages/{enc_message_id}"))
            .await
    }

    pub async fn patch_message(
        &self,
        message_id: &str,
        patch: &GraphMessagePatch,
    ) -> Result<(), String> {
        let prefix = self.api_path_prefix();
        let enc_message_id = urlencoding::encode(message_id);
        self.patch(&format!("{prefix}/messages/{enc_message_id}"), patch)
            .await
    }

    pub async fn move_message(
        &self,
        message_id: &str,
        destination_id: &str,
    ) -> Result<GraphMessage, String> {
        let prefix = self.api_path_prefix();
        let enc_message_id = urlencoding::encode(message_id);
        let body = GraphMoveRequest {
            destination_id: destination_id.to_string(),
        };
        self.post(&format!("{prefix}/messages/{enc_message_id}/move"), &body)
            .await
    }

    pub async fn delete_message(&self, message_id: &str) -> Result<(), String> {
        let prefix = self.api_path_prefix();
        let enc_message_id = urlencoding::encode(message_id);
        self.delete(&format!("{prefix}/messages/{enc_message_id}"))
            .await
    }

    pub async fn get_attachment_bytes(
        &self,
        message_id: &str,
        attachment_id: &str,
    ) -> Result<Vec<u8>, String> {
        let prefix = self.api_path_prefix();
        let enc_message_id = urlencoding::encode(message_id);
        let enc_attachment_id = urlencoding::encode(attachment_id);
        self.get_bytes(&format!(
            "{prefix}/messages/{enc_message_id}/attachments/{enc_attachment_id}/$value"
        ))
        .await
    }

    pub async fn list_outlook_categories(&self) -> Result<Vec<GraphOutlookCategory>, String> {
        let prefix = self.api_path_prefix();
        let mut categories = Vec::new();
        let mut next_url = Some(format!("{prefix}/outlook/masterCategories"));

        while let Some(url) = next_url {
            let page: ODataCollection<GraphOutlookCategory> = if url.starts_with("http") {
                self.get_absolute(&url).await?
            } else {
                self.get_json(&url).await?
            };
            categories.extend(page.value);
            next_url = page.next_link;
        }

        Ok(categories)
    }

    pub async fn list_contact_folders(&self) -> Result<Vec<GraphContactFolder>, String> {
        let prefix = self.api_path_prefix();
        let mut folders = Vec::new();
        let mut next_url = Some(format!("{prefix}/contactFolders?$top=250"));

        while let Some(url) = next_url {
            let page: ODataCollection<GraphContactFolder> = if url.starts_with("http") {
                self.get_absolute(&url).await?
            } else {
                self.get_json(&url).await?
            };
            folders.extend(page.value);
            next_url = page.next_link;
        }

        Ok(folders)
    }

    pub async fn list_contacts(
        &self,
        folder_id: Option<&str>,
    ) -> Result<Vec<GraphContact>, String> {
        let prefix = self.api_path_prefix();
        let initial_url = match folder_id {
            Some(folder_id) => {
                let enc_folder_id = urlencoding::encode(folder_id);
                format!(
                    "{prefix}/contactFolders/{enc_folder_id}/contacts?$select={CONTACT_SELECT}&$top=250"
                )
            }
            None => format!("{prefix}/contacts?$select={CONTACT_SELECT}&$top=250"),
        };

        let mut contacts = Vec::new();
        let mut next_url = Some(initial_url);

        while let Some(url) = next_url {
            let page: ODataCollection<GraphContact> = if url.starts_with("http") {
                self.get_absolute(&url).await?
            } else {
                self.get_json(&url).await?
            };
            contacts.extend(page.value);
            next_url = page.next_link;
        }

        Ok(contacts)
    }
}
