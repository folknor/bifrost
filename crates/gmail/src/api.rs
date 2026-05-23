use serde_json::json;

use crate::Result;
use crate::client::GmailClient;
use crate::types::{
    GmailAttachmentData, GmailDraft, GmailDraftStub, GmailHistoryResponse, GmailLabel,
    GmailMessage, GmailProfile, GmailSendAs, GmailThread, GmailThreadStub, GmailVacationSettings,
    ListDraftsResponse, ListLabelsResponse, ListMessagesResponse, ListSendAsResponse,
    ListThreadsResponse,
};

impl GmailClient {
    // pub: Account uses this to seed cursors; direct callers use it for Gmail profile state.
    pub async fn get_profile(&self) -> Result<GmailProfile> {
        self.get("/profile").await
    }

    // pub: Account uses labels for memberships; direct callers use labels for Gmail management UI.
    pub async fn list_labels(&self) -> Result<Vec<GmailLabel>> {
        let resp: ListLabelsResponse = self.get("/labels").await?;
        Ok(resp.labels)
    }

    // pub: direct Gmail REST callers can create labels outside the sync Account path.
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

    // pub: direct Gmail REST callers can update labels outside the sync Account path.
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

    // pub: direct Gmail REST callers can delete labels outside the sync Account path.
    pub async fn delete_label(&self, label_id: &str) -> Result<()> {
        self.delete(&format!("/labels/{label_id}")).await
    }

    // pub: direct callers need thread listing; Account sync is message-oriented.
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

    // pub: direct callers need message-shaped search and listing.
    pub async fn list_messages(
        &self,
        query: Option<&str>,
        max_results: Option<u32>,
        page_token: Option<&str>,
    ) -> Result<(
        Vec<crate::types::GmailMessageStub>,
        Option<String>,
        Option<i64>,
    )> {
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

        let resp: ListMessagesResponse = self.get(&format!("/messages{qs}")).await?;
        Ok((
            resp.messages,
            resp.next_page_token,
            resp.result_size_estimate,
        ))
    }

    // pub: direct callers need thread hydration; Account sync is message-oriented.
    pub async fn get_thread(&self, thread_id: &str, format: &str) -> Result<GmailThread> {
        self.get(&format!("/threads/{thread_id}?format={format}"))
            .await
    }

    // pub: direct callers can mutate one thread without using bulk Account mutations.
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

    // pub: direct callers can delete one thread without using bulk Account mutations.
    pub async fn delete_thread(&self, thread_id: &str) -> Result<()> {
        self.delete(&format!("/threads/{thread_id}")).await
    }

    // pub: Account get_stream narrows this to sync projections; direct callers choose Gmail format.
    pub async fn get_message(&self, message_id: &str, format: &str) -> Result<GmailMessage> {
        self.get(&format!("/messages/{message_id}?format={format}"))
            .await
    }

    // pub: sending mail is outside the Account sync trait.
    pub async fn send_message(&self, raw: &str, thread_id: Option<&str>) -> Result<GmailMessage> {
        let mut body = json!({ "raw": raw });
        if let Some(tid) = thread_id {
            body["threadId"] = json!(tid);
        }
        self.post("/messages/send", &body).await
    }

    // pub: direct callers can mutate one message without using bulk Account mutations.
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

    // pub: Account open_blob decodes BlobHandles; direct callers can fetch Gmail attachment JSON.
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

    // pub: Account changes_stream wraps history pages; direct callers can inspect raw history.
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

    // pub: draft creation is outside the Account sync trait.
    pub async fn create_draft(&self, raw: &str, thread_id: Option<&str>) -> Result<GmailDraft> {
        let mut message = json!({ "raw": raw });
        if let Some(tid) = thread_id {
            message["threadId"] = json!(tid);
        }
        self.post("/drafts", &json!({ "message": message })).await
    }

    // pub: direct callers can fetch a draft in a chosen Gmail message projection.
    pub async fn get_draft(&self, draft_id: &str, format: &str) -> Result<GmailDraft> {
        self.get(&format!("/drafts/{draft_id}?format={format}"))
            .await
    }

    // pub: draft update is outside the Account sync trait.
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

    // pub: draft deletion is outside the Account sync trait.
    pub async fn delete_draft(&self, draft_id: &str) -> Result<()> {
        self.delete(&format!("/drafts/{draft_id}")).await
    }

    // pub: direct callers can send an existing Gmail draft.
    pub async fn send_draft(&self, draft_id: &str) -> Result<GmailMessage> {
        self.post("/drafts/send", &json!({ "id": draft_id })).await
    }

    // pub: draft listing is outside the Account sync trait.
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

    // pub: send-as management is outside the Account sync trait.
    pub async fn list_send_as(&self) -> Result<Vec<GmailSendAs>> {
        let resp: ListSendAsResponse = self.get("/settings/sendAs").await?;
        Ok(resp.send_as)
    }

    // pub: send-as management is outside the Account sync trait.
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

    // pub: direct callers can patch editable send-as fields.
    pub async fn patch_send_as(
        &self,
        send_as_email: &str,
        body: &serde_json::Value,
    ) -> Result<GmailSendAs> {
        let encoded = bifrost_net::url::encode_component(send_as_email);
        self.patch(&format!("/settings/sendAs/{encoded}"), body)
            .await
    }

    // pub: direct callers can read Gmail vacation responder settings.
    pub async fn get_vacation(&self) -> Result<GmailVacationSettings> {
        self.get("/settings/vacation").await
    }

    // pub: direct callers can replace Gmail vacation responder settings.
    pub async fn update_vacation(
        &self,
        settings: &GmailVacationSettings,
    ) -> Result<GmailVacationSettings> {
        self.put("/settings/vacation", settings).await
    }
}
