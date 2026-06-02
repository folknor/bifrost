use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct GmailMessage {
    pub(crate) id: String,
    pub(crate) thread_id: String,
    #[serde(default)]
    pub(crate) label_ids: Vec<String>,
    #[serde(default)]
    pub(crate) snippet: String,
    pub(crate) history_id: Option<String>,
    pub(crate) internal_date: Option<String>,
    pub(crate) payload: Option<GmailPayload>,
    pub(crate) size_estimate: Option<i64>,
    pub(crate) raw: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct GmailPayload {
    pub(crate) mime_type: String,
    #[serde(default)]
    pub(crate) filename: String,
    #[serde(default)]
    pub(crate) headers: Vec<GmailHeader>,
    pub(crate) body: Option<GmailBody>,
    #[serde(default)]
    pub(crate) parts: Vec<GmailPayload>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct GmailHeader {
    pub(crate) name: String,
    pub(crate) value: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct GmailBody {
    pub(crate) attachment_id: Option<String>,
    #[serde(default)]
    pub(crate) size: i64,
    pub(crate) data: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct GmailThread {
    pub(crate) id: String,
    #[serde(default)]
    pub(crate) messages: Vec<GmailMessage>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct GmailThreadStub {
    pub(crate) id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct GmailLabel {
    pub(crate) id: String,
    pub(crate) name: String,
    #[serde(rename = "type")]
    pub(crate) label_type: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct GmailFilter {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) id: Option<String>,
    pub(crate) criteria: GmailFilterCriteria,
    pub(crate) action: GmailFilterAction,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct GmailFilterCriteria {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) from: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) to: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) subject: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) query: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) negated_query: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) has_attachment: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) exclude_chats: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) size: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) size_comparison: Option<GmailFilterSizeComparison>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum GmailFilterSizeComparison {
    Smaller,
    Larger,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct GmailFilterAction {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) add_label_ids: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) remove_label_ids: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) forward: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct ListFiltersResponse {
    #[serde(default)]
    pub(crate) filter: Vec<GmailFilter>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct GmailHistoryResponse {
    #[serde(default)]
    pub(crate) history: Vec<GmailHistoryItem>,
    pub(crate) history_id: String,
    pub(crate) next_page_token: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct GmailHistoryItem {
    #[serde(default)]
    pub(crate) messages_added: Vec<GmailHistoryMessageWrapper>,
    #[serde(default)]
    pub(crate) messages_deleted: Vec<GmailHistoryMessageWrapper>,
    #[serde(default)]
    pub(crate) labels_added: Vec<GmailHistoryLabelWrapper>,
    #[serde(default)]
    pub(crate) labels_removed: Vec<GmailHistoryLabelWrapper>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct GmailHistoryMessageWrapper {
    pub(crate) message: GmailMessage,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct GmailHistoryLabelWrapper {
    pub(crate) message: GmailMessage,
    pub(crate) label_ids: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ListThreadsResponse {
    #[serde(default)]
    pub(crate) threads: Vec<GmailThreadStub>,
    pub(crate) next_page_token: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ListMessagesResponse {
    #[serde(default)]
    pub(crate) messages: Vec<GmailMessageStub>,
    pub(crate) next_page_token: Option<String>,
    pub(crate) result_size_estimate: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct GmailMessageStub {
    pub(crate) id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ListLabelsResponse {
    #[serde(default)]
    pub(crate) labels: Vec<GmailLabel>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct GmailAttachmentData {
    pub(crate) data: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct GmailDraft {
    pub(crate) id: String,
    pub(crate) message: GmailMessage,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct GmailSendAs {
    pub(crate) send_as_email: String,
    pub(crate) display_name: Option<String>,
    pub(crate) is_default: Option<bool>,
    pub(crate) signature: Option<String>,
    pub(crate) reply_to_address: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ListSendAsResponse {
    #[serde(default)]
    pub(crate) send_as: Vec<GmailSendAs>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct GmailProfile {
    pub(crate) email_address: String,
    pub(crate) history_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct GmailVacationSettings {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) enable_auto_reply: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) response_subject: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) response_body_plain_text: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) response_body_html: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) start_time: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) end_time: Option<String>,
}
