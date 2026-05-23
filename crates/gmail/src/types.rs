use serde::{Deserialize, Serialize};

// pub: direct Gmail REST methods return messages and nested message payloads.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GmailMessage {
    pub id: String,
    pub thread_id: String,
    #[serde(default)]
    pub label_ids: Vec<String>,
    #[serde(default)]
    pub snippet: String,
    pub history_id: Option<String>,
    pub internal_date: Option<String>,
    pub payload: Option<GmailPayload>,
    pub size_estimate: Option<i64>,
    pub raw: Option<String>,
}

// pub: nested in GmailMessage payloads returned by the direct REST facade.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GmailPayload {
    pub part_id: Option<String>,
    pub mime_type: String,
    #[serde(default)]
    pub filename: String,
    #[serde(default)]
    pub headers: Vec<GmailHeader>,
    pub body: Option<GmailBody>,
    #[serde(default)]
    pub parts: Vec<GmailPayload>,
}

// pub: nested in GmailPayload headers returned by the direct REST facade.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GmailHeader {
    pub name: String,
    pub value: String,
}

// pub: nested in GmailPayload bodies returned by the direct REST facade.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GmailBody {
    pub attachment_id: Option<String>,
    #[serde(default)]
    pub size: i64,
    pub data: Option<String>,
}

// pub: returned by GmailClient thread hydration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GmailThread {
    pub id: String,
    pub history_id: Option<String>,
    #[serde(default)]
    pub messages: Vec<GmailMessage>,
}

// pub: returned by GmailClient thread listing.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GmailThreadStub {
    pub id: String,
    pub snippet: Option<String>,
    pub history_id: Option<String>,
}

// pub: returned by GmailClient label methods and used by Account label discovery.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GmailLabel {
    pub id: String,
    pub name: String,
    #[serde(rename = "type")]
    pub label_type: Option<String>,
    pub message_list_visibility: Option<String>,
    pub label_list_visibility: Option<String>,
    pub messages_total: Option<i64>,
    pub messages_unread: Option<i64>,
    pub threads_total: Option<i64>,
    pub threads_unread: Option<i64>,
    pub color: Option<GmailLabelColor>,
}

// pub: nested in GmailLabel responses returned by the direct REST facade.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GmailLabelColor {
    pub text_color: String,
    pub background_color: String,
}

// pub: returned by GmailClient history listing.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GmailHistoryResponse {
    #[serde(default)]
    pub history: Vec<GmailHistoryItem>,
    pub history_id: String,
    pub next_page_token: Option<String>,
}

// pub: nested in GmailHistoryResponse returned by the direct REST facade.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GmailHistoryItem {
    pub id: String,
    #[serde(default)]
    pub messages: Vec<GmailMessage>,
    #[serde(default)]
    pub messages_added: Vec<GmailHistoryMessageWrapper>,
    #[serde(default)]
    pub messages_deleted: Vec<GmailHistoryMessageWrapper>,
    #[serde(default)]
    pub labels_added: Vec<GmailHistoryLabelWrapper>,
    #[serde(default)]
    pub labels_removed: Vec<GmailHistoryLabelWrapper>,
}

// pub: nested in GmailHistoryItem returned by the direct REST facade.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GmailHistoryMessageWrapper {
    pub message: GmailMessage,
}

// pub: nested in GmailHistoryItem returned by the direct REST facade.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GmailHistoryLabelWrapper {
    pub message: GmailMessage,
    pub label_ids: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ListThreadsResponse {
    #[serde(default)]
    pub threads: Vec<GmailThreadStub>,
    pub next_page_token: Option<String>,
    pub result_size_estimate: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ListMessagesResponse {
    #[serde(default)]
    pub messages: Vec<GmailMessageStub>,
    pub next_page_token: Option<String>,
    pub result_size_estimate: Option<i64>,
}

// pub: returned by GmailClient message listing.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GmailMessageStub {
    pub id: String,
    pub thread_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ListLabelsResponse {
    #[serde(default)]
    pub labels: Vec<GmailLabel>,
}

// pub: returned by GmailClient attachment fetches.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GmailAttachmentData {
    pub attachment_id: Option<String>,
    pub size: Option<i64>,
    pub data: String,
}

// pub: returned by GmailClient draft creation and update.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GmailDraft {
    pub id: String,
    pub message: GmailMessage,
}

// pub: returned by GmailClient draft listing.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GmailDraftStub {
    pub id: String,
    pub message: GmailDraftMessageRef,
}

// pub: nested in GmailDraftStub returned by the direct REST facade.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GmailDraftMessageRef {
    pub id: String,
    pub thread_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ListDraftsResponse {
    #[serde(default)]
    pub drafts: Vec<GmailDraftStub>,
    pub next_page_token: Option<String>,
}

// pub: returned by GmailClient send-as listing and update.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GmailSendAs {
    pub send_as_email: String,
    pub display_name: Option<String>,
    pub is_default: Option<bool>,
    pub is_primary: Option<bool>,
    pub treat_as_alias: Option<bool>,
    pub verification_status: Option<String>,
    pub signature: Option<String>,
    pub reply_to_address: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ListSendAsResponse {
    #[serde(default)]
    pub send_as: Vec<GmailSendAs>,
}

// pub: returned by GmailClient profile fetches and used by Account cursor identity checks.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GmailProfile {
    pub email_address: String,
    pub messages_total: Option<i64>,
    pub threads_total: Option<i64>,
    pub history_id: String,
}

// pub: returned by Gmail vacation settings methods.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GmailVacationSettings {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub enable_auto_reply: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub response_subject: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub response_body_plain_text: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub response_body_html: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub restrict_to_contacts: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub restrict_to_domain: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub start_time: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub end_time: Option<String>,
}
