use serde_json::json;

use crate::Result;
use crate::client::GmailClient;
use crate::types::{
    GmailAttachmentData, GmailDraft, GmailDraftStub, GmailHistoryResponse, GmailLabel,
    GmailMessage, GmailProfile, GmailSendAs, GmailThread, GmailThreadStub, ListDraftsResponse,
    ListLabelsResponse, ListSendAsResponse, ListThreadsResponse,
};

impl GmailClient {
    pub async fn get_profile(&self) -> Result<GmailProfile> {
        self.get("/profile").await
    }

    pub async fn list_labels(&self) -> Result<Vec<GmailLabel>> {
        let resp: ListLabelsResponse = self.get("/labels").await?;
        Ok(resp.labels)
    }

    pub async fn create_label(
        &self,
        name: &str,
        color: Option<(&str, &str)>,
    ) -> Result<GmailLabel> {
        let mut body = json!({
            "name": name,
            "labelListVisibility": "labelShow",
            "messageListVisibility": "show",
        });
        if let Some((text_color, bg_color)) = color {
            body["color"] = json!({
                "textColor": text_color,
                "backgroundColor": bg_color,
            });
        }
        self.post("/labels", &body).await
    }

    pub async fn update_label(
        &self,
        label_id: &str,
        name: Option<&str>,
        color: Option<Option<(&str, &str)>>,
    ) -> Result<GmailLabel> {
        let mut body = json!({});
        if let Some(n) = name {
            body["name"] = json!(n);
        }
        if let Some(c) = color {
            body["color"] = match c {
                Some((text, bg)) => json!({"textColor": text, "backgroundColor": bg}),
                None => serde_json::Value::Null,
            };
        }
        self.patch(&format!("/labels/{label_id}"), &body).await
    }

    pub async fn delete_label(&self, label_id: &str) -> Result<()> {
        self.delete(&format!("/labels/{label_id}")).await
    }

    pub async fn list_threads(
        &self,
        query: Option<&str>,
        max_results: Option<u32>,
        page_token: Option<&str>,
    ) -> Result<(Vec<GmailThreadStub>, Option<String>)> {
        let mut params = Vec::new();
        if let Some(q) = query {
            params.push(format!("q={}", bifrost_net::url::encode_component(q)));
        }
        if let Some(max) = max_results {
            params.push(format!("maxResults={max}"));
        }
        if let Some(pt) = page_token {
            params.push(format!("pageToken={pt}"));
        }
        let qs = if params.is_empty() {
            String::new()
        } else {
            format!("?{}", params.join("&"))
        };

        let resp: ListThreadsResponse = self.get(&format!("/threads{qs}")).await?;
        Ok((resp.threads, resp.next_page_token))
    }

    pub async fn get_thread(&self, thread_id: &str, format: &str) -> Result<GmailThread> {
        self.get(&format!("/threads/{thread_id}?format={format}"))
            .await
    }

    pub async fn modify_thread(
        &self,
        thread_id: &str,
        add_labels: &[String],
        remove_labels: &[String],
    ) -> Result<GmailThread> {
        self.post(
            &format!("/threads/{thread_id}/modify"),
            &json!({
                "addLabelIds": add_labels,
                "removeLabelIds": remove_labels,
            }),
        )
        .await
    }

    pub async fn delete_thread(&self, thread_id: &str) -> Result<()> {
        self.delete(&format!("/threads/{thread_id}")).await
    }

    pub async fn get_message(&self, message_id: &str, format: &str) -> Result<GmailMessage> {
        self.get(&format!("/messages/{message_id}?format={format}"))
            .await
    }

    pub async fn send_message(&self, raw: &str, thread_id: Option<&str>) -> Result<GmailMessage> {
        let mut body = json!({ "raw": raw });
        if let Some(tid) = thread_id {
            body["threadId"] = json!(tid);
        }
        self.post("/messages/send", &body).await
    }

    pub async fn modify_message(
        &self,
        message_id: &str,
        add_labels: &[String],
        remove_labels: &[String],
    ) -> Result<GmailMessage> {
        self.post(
            &format!("/messages/{message_id}/modify"),
            &json!({
                "addLabelIds": add_labels,
                "removeLabelIds": remove_labels,
            }),
        )
        .await
    }

    pub async fn get_attachment(
        &self,
        message_id: &str,
        attachment_id: &str,
    ) -> Result<GmailAttachmentData> {
        self.get(&format!(
            "/messages/{message_id}/attachments/{attachment_id}"
        ))
        .await
    }

    pub async fn get_history(
        &self,
        start_history_id: &str,
        page_token: Option<&str>,
    ) -> Result<GmailHistoryResponse> {
        let mut params = vec![
            format!("startHistoryId={start_history_id}"),
            "maxResults=500".to_string(),
            "historyTypes=messageAdded".to_string(),
            "historyTypes=messageDeleted".to_string(),
            "historyTypes=labelAdded".to_string(),
            "historyTypes=labelRemoved".to_string(),
        ];
        if let Some(pt) = page_token {
            params.push(format!("pageToken={pt}"));
        }
        let qs = params.join("&");
        self.get(&format!("/history?{qs}")).await
    }

    pub async fn create_draft(&self, raw: &str, thread_id: Option<&str>) -> Result<GmailDraft> {
        let mut message = json!({ "raw": raw });
        if let Some(tid) = thread_id {
            message["threadId"] = json!(tid);
        }
        self.post("/drafts", &json!({ "message": message })).await
    }

    pub async fn update_draft(
        &self,
        draft_id: &str,
        raw: &str,
        thread_id: Option<&str>,
    ) -> Result<GmailDraft> {
        let mut message = json!({ "raw": raw });
        if let Some(tid) = thread_id {
            message["threadId"] = json!(tid);
        }
        self.put(
            &format!("/drafts/{draft_id}"),
            &json!({ "message": message }),
        )
        .await
    }

    pub async fn delete_draft(&self, draft_id: &str) -> Result<()> {
        self.delete(&format!("/drafts/{draft_id}")).await
    }

    pub async fn list_drafts(&self) -> Result<Vec<GmailDraftStub>> {
        let mut all_drafts = Vec::new();
        let mut page_token: Option<String> = None;

        loop {
            let url = match &page_token {
                Some(pt) => format!("/drafts?maxResults=500&pageToken={pt}"),
                None => "/drafts?maxResults=500".to_string(),
            };
            let resp: ListDraftsResponse = self.get(&url).await?;
            all_drafts.extend(resp.drafts);

            match resp.next_page_token {
                Some(pt) => page_token = Some(pt),
                None => break,
            }
        }

        Ok(all_drafts)
    }

    pub async fn list_send_as(&self) -> Result<Vec<GmailSendAs>> {
        let resp: ListSendAsResponse = self.get("/settings/sendAs").await?;
        Ok(resp.send_as)
    }

    pub async fn update_send_as_signature(
        &self,
        send_as_email: &str,
        signature_html: &str,
    ) -> Result<GmailSendAs> {
        let encoded = bifrost_net::url::encode_component(send_as_email);
        self.put(
            &format!("/settings/sendAs/{encoded}"),
            &json!({ "signature": signature_html }),
        )
        .await
    }
}
