use crate::client::GraphClient;
use crate::types::{GraphMailFolder, GraphProfile, ODataCollection};

impl GraphClient {
    pub(crate) async fn get_profile(&self) -> Result<GraphProfile, String> {
        let prefix = self.api_path_prefix();
        self.get_json(&format!(
            "{prefix}?$select=displayName,mail,userPrincipalName"
        ))
        .await
    }

    pub(crate) async fn list_mail_folders(&self) -> Result<Vec<GraphMailFolder>, String> {
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

    pub(crate) async fn list_mail_folders_recursive(&self) -> Result<Vec<GraphMailFolder>, String> {
        let prefix = self.api_path_prefix();
        let mut folders = self.list_mail_folders().await?;
        let mut queue: std::collections::VecDeque<String> = folders
            .iter()
            .filter(|folder| folder.child_folder_count.unwrap_or(0) > 0)
            .map(|folder| folder.id.clone())
            .collect();

        while let Some(parent_id) = queue.pop_front() {
            let enc_parent_id = bifrost_net::url::encode_component(&parent_id);
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
}
