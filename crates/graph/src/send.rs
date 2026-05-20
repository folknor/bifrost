use mail_parser::MimeHeaders;

use crate::client::GraphClient;
use crate::encoding::{decode_base64url_nopad, encode_base64_standard};
use crate::types::{
    CreateUploadSessionRequest, GraphAttachmentInput, GraphBodyInput, GraphCreateMessage,
    GraphEmailAddress, GraphRecipient, SingleValueExtendedProperty, UploadSession,
    UploadSessionAttachmentItem,
};

const PID_TAG_DEFERRED_SEND_TIME: &str = "SystemTime 0x3FEF";
const GRAPH_INLINE_ATTACHMENT_LIMIT: usize = 3 * 1024 * 1024;
const UPLOAD_CHUNK_SIZE: usize = 4 * 1024 * 1024;

pub async fn send_via_draft(client: &GraphClient, raw_base64url: &str) -> Result<String, String> {
    let draft_id = create_draft(client, raw_base64url).await?;
    let enc_draft_id = urlencoding::encode(&draft_id);
    let me = client.api_path_prefix();
    client
        .post_no_content::<()>(&format!("{me}/messages/{enc_draft_id}/send"), None)
        .await?;
    Ok(draft_id)
}

pub async fn create_draft(client: &GraphClient, raw_base64url: &str) -> Result<String, String> {
    create_draft_inner(client, raw_base64url, None).await
}

pub async fn create_draft_with_deferred_time(
    client: &GraphClient,
    raw_base64url: &str,
    send_at_utc: &str,
) -> Result<String, String> {
    create_draft_inner(client, raw_base64url, Some(send_at_utc)).await
}

async fn create_draft_inner(
    client: &GraphClient,
    raw_base64url: &str,
    deferred_send_time: Option<&str>,
) -> Result<String, String> {
    let raw_bytes = decode_base64url_nopad(raw_base64url)?;
    let parsed = mail_parser::MessageParser::default()
        .parse(&raw_bytes)
        .ok_or_else(|| "Failed to parse MIME message".to_string())?;

    let mut create_msg = mime_to_graph_message(&parsed)?;
    if let Some(send_at_utc) = deferred_send_time {
        create_msg.single_value_extended_properties = Some(vec![SingleValueExtendedProperty {
            id: PID_TAG_DEFERRED_SEND_TIME.to_string(),
            value: send_at_utc.to_string(),
        }]);
    }

    let me = client.api_path_prefix();
    let draft: serde_json::Value = client.post(&format!("{me}/messages"), &create_msg).await?;
    let draft_id = draft
        .get("id")
        .and_then(|v| v.as_str())
        .ok_or_else(|| "Draft response missing id".to_string())?
        .to_string();

    upload_attachments_from_mime(client, &draft_id, &parsed).await?;
    Ok(draft_id)
}

pub fn mime_to_graph_message(
    parsed: &mail_parser::Message<'_>,
) -> Result<GraphCreateMessage, String> {
    let subject = parsed.subject().map(String::from);
    let body = if let Some(html) = parsed.body_html(0) {
        Some(GraphBodyInput {
            content_type: "html".to_string(),
            content: html.to_string(),
        })
    } else {
        parsed.body_text(0).map(|text| GraphBodyInput {
            content_type: "text".to_string(),
            content: text.to_string(),
        })
    };

    Ok(GraphCreateMessage {
        subject,
        body,
        to_recipients: addr_to_recipients(parsed.to()),
        cc_recipients: addr_to_recipients(parsed.cc()),
        bcc_recipients: addr_to_recipients(parsed.bcc()),
        reply_to: addr_to_recipients(parsed.reply_to()),
        importance: None,
        internet_message_id: parsed.message_id().map(String::from),
        single_value_extended_properties: None,
        from: None,
        sender: None,
        is_read_receipt_requested: None,
    })
}

fn addr_to_recipients(addr: Option<&mail_parser::Address<'_>>) -> Option<Vec<GraphRecipient>> {
    let addr = addr?;
    let recips: Vec<GraphRecipient> = addr
        .iter()
        .filter_map(|group| {
            group.address.as_ref().map(|email| GraphRecipient {
                email_address: GraphEmailAddress {
                    name: group.name.as_ref().map(std::string::ToString::to_string),
                    address: email.to_string(),
                },
            })
        })
        .collect();
    if recips.is_empty() {
        None
    } else {
        Some(recips)
    }
}

async fn upload_attachments_from_mime(
    client: &GraphClient,
    draft_id: &str,
    parsed: &mail_parser::Message<'_>,
) -> Result<(), String> {
    let enc_draft_id = urlencoding::encode(draft_id);
    for attachment in parsed.attachments() {
        let name = attachment
            .attachment_name()
            .unwrap_or("attachment")
            .to_string();
        let content_type = attachment
            .content_type()
            .map(|ct| {
                if let Some(st) = ct.subtype() {
                    format!("{}/{st}", ct.ctype())
                } else {
                    ct.ctype().to_string()
                }
            })
            .unwrap_or_else(|| "application/octet-stream".to_string());
        let is_inline = attachment
            .content_disposition()
            .is_some_and(|d| d.ctype() == "inline");
        let raw_bytes = attachment.contents();

        if raw_bytes.len() > GRAPH_INLINE_ATTACHMENT_LIMIT {
            upload_large_attachment(
                client,
                &enc_draft_id,
                &name,
                &content_type,
                is_inline,
                raw_bytes,
            )
            .await?;
        } else {
            upload_inline_attachment(
                client,
                &enc_draft_id,
                name,
                content_type,
                is_inline,
                attachment.content_id(),
                raw_bytes,
            )
            .await?;
        }
    }

    Ok(())
}

async fn upload_inline_attachment(
    client: &GraphClient,
    enc_draft_id: &str,
    name: String,
    content_type: String,
    is_inline: bool,
    content_id: Option<&str>,
    raw_bytes: &[u8],
) -> Result<(), String> {
    let input = GraphAttachmentInput {
        odata_type: "#microsoft.graph.fileAttachment".to_string(),
        name,
        content_type,
        content_bytes: encode_base64_standard(raw_bytes),
        is_inline: if is_inline { Some(true) } else { None },
        content_id: content_id.map(|id| id.trim_matches(&['<', '>'] as &[char]).to_string()),
    };

    let me = client.api_path_prefix();
    let _: serde_json::Value = client
        .post(&format!("{me}/messages/{enc_draft_id}/attachments"), &input)
        .await?;
    Ok(())
}

async fn upload_large_attachment(
    client: &GraphClient,
    enc_draft_id: &str,
    name: &str,
    content_type: &str,
    is_inline: bool,
    data: &[u8],
) -> Result<(), String> {
    let size =
        i64::try_from(data.len()).map_err(|_| "Attachment too large to upload".to_string())?;
    let session_req = CreateUploadSessionRequest {
        attachment_item: UploadSessionAttachmentItem {
            odata_type: "#microsoft.graph.fileAttachment".to_string(),
            name: name.to_string(),
            size,
            content_type: Some(content_type.to_string()),
            is_inline: if is_inline { Some(true) } else { None },
        },
    };

    let me = client.api_path_prefix();
    let session: UploadSession = client
        .post(
            &format!("{me}/messages/{enc_draft_id}/attachments/createUploadSession"),
            &session_req,
        )
        .await?;

    let total = data.len();
    let mut offset = 0;
    while offset < total {
        let end = (offset + UPLOAD_CHUNK_SIZE).min(total);
        let chunk = &data[offset..end];
        client
            .put_bytes_range(&session.upload_url, chunk, offset, end - 1, total)
            .await?;
        offset = end;
    }

    Ok(())
}
