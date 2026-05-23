use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize)]
pub(crate) struct ODataCollection<T> {
    pub(crate) value: Vec<T>,
    #[serde(rename = "@odata.nextLink")]
    pub(crate) next_link: Option<String>,
    #[serde(rename = "@odata.deltaLink")]
    pub(crate) delta_link: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct GraphMailFolder {
    pub(crate) id: String,
    pub(crate) display_name: Option<String>,
    pub(crate) child_folder_count: Option<i32>,
    pub(crate) parent_folder_id: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct GraphProfile {
    pub(crate) display_name: Option<String>,
    pub(crate) mail: Option<String>,
    pub(crate) user_principal_name: Option<String>,
}

/// The `$select` fields we request for sync messages.
pub(crate) const MESSAGE_SELECT: &str = "\
id,conversationId,subject,bodyPreview,body,uniqueBody,from,\
toRecipients,ccRecipients,bccRecipients,replyTo,\
receivedDateTime,sentDateTime,isRead,isDraft,hasAttachments,\
importance,parentFolderId,categories,flag,\
inferenceClassification,isReadReceiptRequested,internetMessageHeaders,internetMessageId";

/// The `$select` fields we request for contact sync.
pub(crate) const CONTACT_SELECT: &str = "id,displayName,emailAddresses,parentFolderId";

#[derive(Debug, Clone, Serialize)]
pub(crate) struct BatchRequestItem {
    pub(crate) id: String,
    pub(crate) method: String,
    pub(crate) url: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) body: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) headers: Option<std::collections::HashMap<String, String>>,
}

#[derive(Debug, Serialize)]
pub(crate) struct BatchRequest {
    pub(crate) requests: Vec<BatchRequestItem>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct BatchResponseItem {
    pub(crate) id: String,
    pub(crate) status: u16,
    #[serde(default)]
    pub(crate) headers: Option<std::collections::HashMap<String, String>>,
    pub(crate) body: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct BatchResponse {
    pub(crate) responses: Vec<BatchResponseItem>,
}
