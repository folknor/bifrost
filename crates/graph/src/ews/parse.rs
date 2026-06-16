//! EWS read-operation result types and quick-xml response parsers.
//!
//! Copied and adapted from ratatoskr's `ews/parsers.rs` + `ews/mod.rs`:
//! the only reshaping is the error boundary. Where ratatoskr returned a
//! bare `String`, these return `EwsError` so the account layer routes
//! the parse miss through `ews_error_to_account_error` as
//! `Protocol(ParseFailed)` (a `MalformedXml` carrier), identical in
//! shape to `check_soap_fault`. Every type is `pub(crate)`: the cursor
//! payload and routing stay private to the graph crate.
//!
//! Some result-struct fields and the `GetItem` parser are part of this
//! foundation but only consumed by the hydration/discovery paths that
//! wire on top of the read ops; they are parsed in full now so the data
//! is ready when those callers land.
#![allow(dead_code)]

use base64::{Engine, engine::general_purpose::STANDARD as BASE64};
use bifrost_types::DiagnosticText;
use quick_xml::Reader;
use quick_xml::escape::unescape;
use quick_xml::events::Event;

use super::EwsError;
use super::xml_helpers::{extract_attribute, push_general_ref, strip_ns};

// ── Result types ────────────────────────────────────────────

#[derive(Debug, Clone)]
pub(crate) struct EwsFolder {
    pub(crate) folder_id: String,
    pub(crate) display_name: String,
    pub(crate) folder_class: Option<String>,
    pub(crate) total_count: u32,
    pub(crate) unread_count: u32,
    pub(crate) child_folder_count: u32,
    pub(crate) effective_rights: EwsEffectiveRights,
    pub(crate) replica_list: Option<Vec<u8>>,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct EwsEffectiveRights {
    pub(crate) create_associated: bool,
    pub(crate) create_contents: bool,
    pub(crate) create_hierarchy: bool,
    pub(crate) delete: bool,
    pub(crate) modify: bool,
    pub(crate) read: bool,
}

#[derive(Debug, Clone)]
pub(crate) struct EwsItem {
    pub(crate) item_id: String,
    pub(crate) change_key: Option<String>,
    pub(crate) subject: Option<String>,
    pub(crate) sender_email: Option<String>,
    pub(crate) sender_name: Option<String>,
    /// ISO-8601 `DateTimeReceived` as the server reported it.
    pub(crate) received_at: Option<String>,
    pub(crate) body_preview: Option<String>,
    pub(crate) body_html: Option<String>,
    pub(crate) is_read: bool,
    pub(crate) item_class: String,
    pub(crate) to_recipients: Vec<EwsRecipient>,
    pub(crate) cc_recipients: Vec<EwsRecipient>,
}

#[derive(Debug, Clone)]
pub(crate) struct EwsRecipient {
    pub(crate) email: String,
    pub(crate) name: Option<String>,
}

pub(crate) struct FindItemsResult {
    pub(crate) items: Vec<EwsItem>,
    pub(crate) total_count: u32,
    pub(crate) includes_last: bool,
}

fn malformed(detail: impl Into<String>) -> EwsError {
    EwsError::MalformedXml(DiagnosticText::support_only(detail.into()))
}

// ── PR_REPLICA_LIST decoding ────────────────────────────────

/// Decode `PR_REPLICA_LIST` (0x6698) binary data into GUID strings.
///
/// The binary format is a sequence of null-terminated ASCII hex GUID
/// strings, each `{XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX}\0`.
pub(crate) fn decode_replica_list(base64_data: &str) -> Result<Vec<String>, EwsError> {
    let bytes = BASE64
        .decode(base64_data)
        .map_err(|e| malformed(format!("Failed to decode base64 replica list: {e}")))?;

    let mut guids = Vec::new();
    let mut start = 0;

    for (i, &b) in bytes.iter().enumerate() {
        if b == 0 {
            if i > start
                && let Ok(s) = std::str::from_utf8(&bytes[start..i])
            {
                let trimmed = s.trim();
                if !trimmed.is_empty() {
                    guids.push(trimmed.to_string());
                }
            }
            start = i + 1;
        }
    }

    Ok(guids)
}

// ── Response parsers ────────────────────────────────────────

pub(crate) fn parse_find_folder_response(xml: &str) -> Result<Vec<EwsFolder>, EwsError> {
    let mut reader = Reader::from_str(xml);
    let mut folders = Vec::new();

    let mut in_folder = false;
    let mut in_effective_rights = false;
    let mut current_tag = String::new();
    let mut buf = String::new();

    let mut folder_id = String::new();
    let mut display_name = String::new();
    let mut folder_class: Option<String> = None;
    let mut total_count: u32 = 0;
    let mut unread_count: u32 = 0;
    let mut child_folder_count: u32 = 0;
    let mut rights = EwsEffectiveRights::default();

    loop {
        match reader.read_event() {
            Ok(Event::Start(ref e)) => {
                let name = String::from_utf8_lossy(e.name().as_ref()).to_string();
                let local = strip_ns(&name);

                if is_folder_tag(local) {
                    in_folder = true;
                    folder_id.clear();
                    display_name.clear();
                    folder_class = None;
                    total_count = 0;
                    unread_count = 0;
                    child_folder_count = 0;
                    rights = EwsEffectiveRights::default();
                }
                if in_folder && local == "EffectiveRights" {
                    in_effective_rights = true;
                }
                current_tag = local.to_string();
                buf.clear();

                if in_folder && local == "FolderId" {
                    folder_id = extract_attribute(e, "Id");
                }
            }
            Ok(Event::Empty(ref e)) => {
                let name = String::from_utf8_lossy(e.name().as_ref()).to_string();
                let local = strip_ns(&name);
                if in_folder && local == "FolderId" {
                    folder_id = extract_attribute(e, "Id");
                }
            }
            Ok(Event::Text(ref e)) => push_text(e, &mut buf),
            Ok(Event::GeneralRef(ref e)) => push_general_ref(e, &mut buf),
            Ok(Event::End(ref e)) => {
                let name = String::from_utf8_lossy(e.name().as_ref()).to_string();
                let local = strip_ns(&name);
                let trimmed = buf.trim();

                if in_effective_rights {
                    apply_effective_right(&mut rights, &current_tag, trimmed);
                    if local == "EffectiveRights" {
                        in_effective_rights = false;
                    }
                } else if in_folder {
                    match current_tag.as_str() {
                        "DisplayName" => display_name = trimmed.to_string(),
                        "FolderClass" => folder_class = Some(trimmed.to_string()),
                        "TotalCount" => total_count = trimmed.parse().unwrap_or(0),
                        "UnreadCount" => unread_count = trimmed.parse().unwrap_or(0),
                        "ChildFolderCount" => child_folder_count = trimmed.parse().unwrap_or(0),
                        _ => {}
                    }
                }

                if is_folder_tag(local) && in_folder {
                    if !folder_id.is_empty() {
                        folders.push(EwsFolder {
                            folder_id: folder_id.clone(),
                            display_name: display_name.clone(),
                            folder_class: folder_class.clone(),
                            total_count,
                            unread_count,
                            child_folder_count,
                            effective_rights: rights.clone(),
                            replica_list: None,
                        });
                    }
                    in_folder = false;
                }

                buf.clear();
                current_tag.clear();
            }
            Ok(Event::Eof) => break,
            Err(e) => return Err(malformed(format!("FindFolder parse failed: {e}"))),
            _ => {}
        }
    }

    Ok(folders)
}

pub(crate) fn parse_get_folder_response(xml: &str) -> Result<EwsFolder, EwsError> {
    let mut reader = Reader::from_str(xml);

    let mut in_folder = false;
    let mut in_effective_rights = false;
    let mut in_extended_property = false;
    let mut current_tag = String::new();
    let mut buf = String::new();

    let mut folder_id = String::new();
    let mut display_name = String::new();
    let mut folder_class: Option<String> = None;
    let mut total_count: u32 = 0;
    let mut unread_count: u32 = 0;
    let mut child_folder_count: u32 = 0;
    let mut rights = EwsEffectiveRights::default();
    let mut replica_list: Option<Vec<u8>> = None;

    loop {
        match reader.read_event() {
            Ok(Event::Start(ref e)) => {
                let name = String::from_utf8_lossy(e.name().as_ref()).to_string();
                let local = strip_ns(&name);

                if is_folder_tag(local) {
                    in_folder = true;
                }
                if in_folder && local == "EffectiveRights" {
                    in_effective_rights = true;
                }
                if in_folder && local == "ExtendedProperty" {
                    in_extended_property = true;
                }
                current_tag = local.to_string();
                buf.clear();

                if in_folder && local == "FolderId" {
                    folder_id = extract_attribute(e, "Id");
                }
            }
            Ok(Event::Empty(ref e)) => {
                let name = String::from_utf8_lossy(e.name().as_ref()).to_string();
                let local = strip_ns(&name);
                if in_folder && local == "FolderId" {
                    folder_id = extract_attribute(e, "Id");
                }
            }
            Ok(Event::Text(ref e)) => push_text(e, &mut buf),
            Ok(Event::GeneralRef(ref e)) => push_general_ref(e, &mut buf),
            Ok(Event::End(ref e)) => {
                let name = String::from_utf8_lossy(e.name().as_ref()).to_string();
                let local = strip_ns(&name);
                let trimmed = buf.trim();

                if in_effective_rights {
                    apply_effective_right(&mut rights, &current_tag, trimmed);
                    if local == "EffectiveRights" {
                        in_effective_rights = false;
                    }
                } else if in_extended_property {
                    if current_tag == "Value"
                        && !trimmed.is_empty()
                        && let Ok(bytes) = BASE64.decode(trimmed)
                    {
                        replica_list = Some(bytes);
                    }
                    if local == "ExtendedProperty" {
                        in_extended_property = false;
                    }
                } else if in_folder {
                    match current_tag.as_str() {
                        "DisplayName" => display_name = trimmed.to_string(),
                        "FolderClass" => folder_class = Some(trimmed.to_string()),
                        "TotalCount" => total_count = trimmed.parse().unwrap_or(0),
                        "UnreadCount" => unread_count = trimmed.parse().unwrap_or(0),
                        "ChildFolderCount" => child_folder_count = trimmed.parse().unwrap_or(0),
                        _ => {}
                    }
                }

                buf.clear();
                current_tag.clear();
            }
            Ok(Event::Eof) => break,
            Err(e) => return Err(malformed(format!("GetFolder parse failed: {e}"))),
            _ => {}
        }
    }

    if folder_id.is_empty() {
        return Err(malformed("No folder found in GetFolder response"));
    }

    Ok(EwsFolder {
        folder_id,
        display_name,
        folder_class,
        total_count,
        unread_count,
        child_folder_count,
        effective_rights: rights,
        replica_list,
    })
}

pub(crate) fn parse_find_items_response(xml: &str) -> Result<FindItemsResult, EwsError> {
    let mut reader = Reader::from_str(xml);
    let mut items = Vec::new();

    let mut total_count: u32 = 0;
    let mut includes_last = false;

    let mut in_message = false;
    let mut in_from = false;
    let mut in_mailbox = false;
    let mut current_tag = String::new();
    let mut buf = String::new();

    let mut item_id = String::new();
    let mut change_key: Option<String> = None;
    let mut subject: Option<String> = None;
    let mut sender_email: Option<String> = None;
    let mut sender_name: Option<String> = None;
    let mut received_at: Option<String> = None;
    let mut body_preview: Option<String> = None;
    let mut is_read = false;
    let mut item_class = String::new();

    loop {
        match reader.read_event() {
            Ok(Event::Start(ref e)) => {
                let name = String::from_utf8_lossy(e.name().as_ref()).to_string();
                let local = strip_ns(&name);

                if local == "Message" {
                    in_message = true;
                    item_id.clear();
                    change_key = None;
                    subject = None;
                    sender_email = None;
                    sender_name = None;
                    received_at = None;
                    body_preview = None;
                    is_read = false;
                    item_class.clear();
                }
                if in_message && local == "From" {
                    in_from = true;
                }
                if in_from && local == "Mailbox" {
                    in_mailbox = true;
                }
                if local == "RootFolder" {
                    total_count = extract_attribute(e, "TotalItemsInView")
                        .parse()
                        .unwrap_or(0);
                    includes_last = extract_attribute(e, "IncludesLastItemInRange") == "true";
                }

                current_tag = local.to_string();
                buf.clear();

                if in_message && local == "ItemId" {
                    item_id = extract_attribute(e, "Id");
                    change_key = Some(extract_attribute(e, "ChangeKey")).filter(|s| !s.is_empty());
                }
            }
            Ok(Event::Empty(ref e)) => {
                let name = String::from_utf8_lossy(e.name().as_ref()).to_string();
                let local = strip_ns(&name);
                if in_message && local == "ItemId" {
                    item_id = extract_attribute(e, "Id");
                    change_key = Some(extract_attribute(e, "ChangeKey")).filter(|s| !s.is_empty());
                }
                if local == "RootFolder" {
                    total_count = extract_attribute(e, "TotalItemsInView")
                        .parse()
                        .unwrap_or(0);
                    includes_last = extract_attribute(e, "IncludesLastItemInRange") == "true";
                }
            }
            Ok(Event::Text(ref e)) => push_text(e, &mut buf),
            Ok(Event::GeneralRef(ref e)) => push_general_ref(e, &mut buf),
            Ok(Event::End(ref e)) => {
                let name = String::from_utf8_lossy(e.name().as_ref()).to_string();
                let local = strip_ns(&name);
                let trimmed = buf.trim();

                if in_mailbox && in_from {
                    match current_tag.as_str() {
                        "EmailAddress" => sender_email = Some(trimmed.to_string()),
                        "Name" => sender_name = Some(trimmed.to_string()),
                        _ => {}
                    }
                    if local == "Mailbox" {
                        in_mailbox = false;
                    }
                } else if in_message {
                    match current_tag.as_str() {
                        "Subject" => subject = Some(trimmed.to_string()),
                        "DateTimeReceived" => received_at = Some(trimmed.to_string()),
                        "Preview" => body_preview = Some(trimmed.to_string()),
                        "IsRead" => is_read = trimmed == "true",
                        "ItemClass" => item_class = trimmed.to_string(),
                        _ => {}
                    }
                }

                if local == "From" {
                    in_from = false;
                }
                if local == "Message" && in_message {
                    if !item_id.is_empty() {
                        items.push(EwsItem {
                            item_id: item_id.clone(),
                            change_key: change_key.clone(),
                            subject: subject.clone(),
                            sender_email: sender_email.clone(),
                            sender_name: sender_name.clone(),
                            received_at: received_at.clone(),
                            body_preview: body_preview.clone(),
                            body_html: None,
                            is_read,
                            item_class: default_item_class(&item_class),
                            to_recipients: Vec::new(),
                            cc_recipients: Vec::new(),
                        });
                    }
                    in_message = false;
                }

                buf.clear();
                current_tag.clear();
            }
            Ok(Event::Eof) => break,
            Err(e) => return Err(malformed(format!("FindItem parse failed: {e}"))),
            _ => {}
        }
    }

    Ok(FindItemsResult {
        items,
        total_count,
        includes_last,
    })
}

pub(crate) fn parse_get_item_response(xml: &str) -> Result<EwsItem, EwsError> {
    let mut reader = Reader::from_str(xml);

    let mut in_message = false;
    let mut in_from = false;
    let mut in_to = false;
    let mut in_cc = false;
    let mut in_mailbox = false;
    let mut current_tag = String::new();
    let mut buf = String::new();

    let mut item_id = String::new();
    let mut change_key: Option<String> = None;
    let mut subject: Option<String> = None;
    let mut sender_email: Option<String> = None;
    let mut sender_name: Option<String> = None;
    let mut received_at: Option<String> = None;
    let mut body_html: Option<String> = None;
    let mut is_read = false;
    let mut item_class = String::new();
    let mut to_recipients: Vec<EwsRecipient> = Vec::new();
    let mut cc_recipients: Vec<EwsRecipient> = Vec::new();

    let mut recip_email = String::new();
    let mut recip_name: Option<String> = None;

    loop {
        match reader.read_event() {
            Ok(Event::Start(ref e)) => {
                let name = String::from_utf8_lossy(e.name().as_ref()).to_string();
                let local = strip_ns(&name);

                if local == "Message" {
                    in_message = true;
                }
                if in_message {
                    match local {
                        "From" => in_from = true,
                        "ToRecipients" => in_to = true,
                        "CcRecipients" => in_cc = true,
                        _ => {}
                    }
                }
                if (in_from || in_to || in_cc) && local == "Mailbox" {
                    in_mailbox = true;
                    recip_email.clear();
                    recip_name = None;
                }

                current_tag = local.to_string();
                buf.clear();

                if in_message && local == "ItemId" {
                    item_id = extract_attribute(e, "Id");
                    change_key = Some(extract_attribute(e, "ChangeKey")).filter(|s| !s.is_empty());
                }
            }
            Ok(Event::Empty(ref e)) => {
                let name = String::from_utf8_lossy(e.name().as_ref()).to_string();
                let local = strip_ns(&name);
                if in_message && local == "ItemId" {
                    item_id = extract_attribute(e, "Id");
                    change_key = Some(extract_attribute(e, "ChangeKey")).filter(|s| !s.is_empty());
                }
            }
            Ok(Event::Text(ref e)) => push_text(e, &mut buf),
            Ok(Event::GeneralRef(ref e)) => push_general_ref(e, &mut buf),
            Ok(Event::End(ref e)) => {
                let name = String::from_utf8_lossy(e.name().as_ref()).to_string();
                let local = strip_ns(&name);
                let trimmed = buf.trim();

                if in_mailbox {
                    match current_tag.as_str() {
                        "EmailAddress" => recip_email = trimmed.to_string(),
                        "Name" => recip_name = Some(trimmed.to_string()),
                        _ => {}
                    }
                    if local == "Mailbox" {
                        in_mailbox = false;
                        if !recip_email.is_empty() {
                            let recipient = EwsRecipient {
                                email: recip_email.clone(),
                                name: recip_name.clone(),
                            };
                            if in_from {
                                sender_email = Some(recip_email.clone());
                                sender_name = recip_name.clone();
                            } else if in_to {
                                to_recipients.push(recipient);
                            } else if in_cc {
                                cc_recipients.push(recipient);
                            }
                        }
                    }
                } else if in_message {
                    match current_tag.as_str() {
                        "Subject" => subject = Some(trimmed.to_string()),
                        "DateTimeReceived" => received_at = Some(trimmed.to_string()),
                        "Body" => body_html = Some(trimmed.to_string()),
                        "IsRead" => is_read = trimmed == "true",
                        "ItemClass" => item_class = trimmed.to_string(),
                        _ => {}
                    }
                }

                match local {
                    "From" => in_from = false,
                    "ToRecipients" => in_to = false,
                    "CcRecipients" => in_cc = false,
                    _ => {}
                }

                buf.clear();
                current_tag.clear();
            }
            Ok(Event::Eof) => break,
            Err(e) => return Err(malformed(format!("GetItem parse failed: {e}"))),
            _ => {}
        }
    }

    if item_id.is_empty() {
        return Err(malformed("No item found in GetItem response"));
    }

    Ok(EwsItem {
        item_id,
        change_key,
        subject,
        sender_email,
        sender_name,
        received_at,
        body_preview: None,
        body_html,
        is_read,
        item_class: default_item_class(&item_class),
        to_recipients,
        cc_recipients,
    })
}

// ── Shared helpers ──────────────────────────────────────────

fn is_folder_tag(local: &str) -> bool {
    matches!(
        local,
        "Folder" | "ContactsFolder" | "CalendarFolder" | "TasksFolder"
    )
}

fn apply_effective_right(rights: &mut EwsEffectiveRights, tag: &str, value: &str) {
    let on = value == "true";
    match tag {
        "CreateAssociated" => rights.create_associated = on,
        "CreateContents" => rights.create_contents = on,
        "CreateHierarchy" => rights.create_hierarchy = on,
        "Delete" => rights.delete = on,
        "Modify" => rights.modify = on,
        "Read" => rights.read = on,
        _ => {}
    }
}

fn default_item_class(item_class: &str) -> String {
    if item_class.is_empty() {
        "IPM.Note".to_string()
    } else {
        item_class.to_string()
    }
}

fn push_text(e: &quick_xml::events::BytesText<'_>, buf: &mut String) {
    if let Ok(raw) = std::str::from_utf8(e.as_ref())
        && let Ok(text) = unescape(raw)
    {
        buf.push_str(&text);
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn parse_find_folder_response_two_folders() {
        let xml = r#"<?xml version="1.0" encoding="utf-8"?>
<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/">
  <s:Body>
    <m:FindFolderResponse xmlns:m="http://schemas.microsoft.com/exchange/services/2006/messages"
                          xmlns:t="http://schemas.microsoft.com/exchange/services/2006/types">
      <m:ResponseMessages>
        <m:FindFolderResponseMessage ResponseClass="Success">
          <m:ResponseCode>NoError</m:ResponseCode>
          <m:RootFolder TotalItemsInView="2" IncludesLastItemInRange="true">
            <t:Folders>
              <t:Folder>
                <t:FolderId Id="AAMkAGFk=" ChangeKey="AQAAAB"/>
                <t:DisplayName>Company Announcements</t:DisplayName>
                <t:TotalCount>42</t:TotalCount>
                <t:ChildFolderCount>3</t:ChildFolderCount>
                <t:UnreadCount>5</t:UnreadCount>
                <t:FolderClass>IPF.Note</t:FolderClass>
                <t:EffectiveRights>
                  <t:CreateAssociated>false</t:CreateAssociated>
                  <t:CreateContents>true</t:CreateContents>
                  <t:CreateHierarchy>false</t:CreateHierarchy>
                  <t:Delete>false</t:Delete>
                  <t:Modify>false</t:Modify>
                  <t:Read>true</t:Read>
                </t:EffectiveRights>
              </t:Folder>
              <t:Folder>
                <t:FolderId Id="BBNkAHJk=" ChangeKey="BQAAAC"/>
                <t:DisplayName>IT Helpdesk</t:DisplayName>
                <t:TotalCount>128</t:TotalCount>
                <t:ChildFolderCount>0</t:ChildFolderCount>
                <t:UnreadCount>12</t:UnreadCount>
                <t:FolderClass>IPF.Note</t:FolderClass>
                <t:EffectiveRights>
                  <t:CreateAssociated>true</t:CreateAssociated>
                  <t:CreateContents>true</t:CreateContents>
                  <t:CreateHierarchy>true</t:CreateHierarchy>
                  <t:Delete>true</t:Delete>
                  <t:Modify>true</t:Modify>
                  <t:Read>true</t:Read>
                </t:EffectiveRights>
              </t:Folder>
            </t:Folders>
          </m:RootFolder>
        </m:FindFolderResponseMessage>
      </m:ResponseMessages>
    </m:FindFolderResponse>
  </s:Body>
</s:Envelope>"#;

        let folders = parse_find_folder_response(xml).expect("parse should succeed");
        assert_eq!(folders.len(), 2);
        assert_eq!(folders[0].folder_id, "AAMkAGFk=");
        assert_eq!(folders[0].display_name, "Company Announcements");
        assert_eq!(folders[0].total_count, 42);
        assert_eq!(folders[0].unread_count, 5);
        assert_eq!(folders[0].child_folder_count, 3);
        assert_eq!(folders[0].folder_class.as_deref(), Some("IPF.Note"));
        assert!(folders[0].effective_rights.read);
        assert!(folders[0].effective_rights.create_contents);
        assert!(!folders[0].effective_rights.delete);
        assert!(!folders[0].effective_rights.modify);
        assert_eq!(folders[1].folder_id, "BBNkAHJk=");
        assert_eq!(folders[1].display_name, "IT Helpdesk");
        assert_eq!(folders[1].total_count, 128);
        assert!(folders[1].effective_rights.delete);
        assert!(folders[1].effective_rights.modify);
    }

    #[test]
    fn parse_find_folder_response_empty() {
        let xml = r#"<?xml version="1.0" encoding="utf-8"?>
<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/">
  <s:Body>
    <m:FindFolderResponse xmlns:m="http://schemas.microsoft.com/exchange/services/2006/messages"
                          xmlns:t="http://schemas.microsoft.com/exchange/services/2006/types">
      <m:ResponseMessages>
        <m:FindFolderResponseMessage ResponseClass="Success">
          <m:ResponseCode>NoError</m:ResponseCode>
          <m:RootFolder TotalItemsInView="0" IncludesLastItemInRange="true">
            <t:Folders/>
          </m:RootFolder>
        </m:FindFolderResponseMessage>
      </m:ResponseMessages>
    </m:FindFolderResponse>
  </s:Body>
</s:Envelope>"#;
        let folders = parse_find_folder_response(xml).expect("parse should succeed");
        assert!(folders.is_empty());
    }

    #[test]
    fn parse_find_folder_response_effective_rights_parsing() {
        let xml = r#"<?xml version="1.0" encoding="utf-8"?>
<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/">
  <s:Body>
    <m:FindFolderResponse xmlns:m="http://schemas.microsoft.com/exchange/services/2006/messages"
                          xmlns:t="http://schemas.microsoft.com/exchange/services/2006/types">
      <m:ResponseMessages>
        <m:FindFolderResponseMessage ResponseClass="Success">
          <m:ResponseCode>NoError</m:ResponseCode>
          <m:RootFolder TotalItemsInView="1" IncludesLastItemInRange="true">
            <t:Folders>
              <t:Folder>
                <t:FolderId Id="AARead="/>
                <t:DisplayName>ReadOnly Folder</t:DisplayName>
                <t:TotalCount>10</t:TotalCount>
                <t:ChildFolderCount>0</t:ChildFolderCount>
                <t:UnreadCount>0</t:UnreadCount>
                <t:EffectiveRights>
                  <t:CreateAssociated>false</t:CreateAssociated>
                  <t:CreateContents>false</t:CreateContents>
                  <t:CreateHierarchy>false</t:CreateHierarchy>
                  <t:Delete>false</t:Delete>
                  <t:Modify>false</t:Modify>
                  <t:Read>true</t:Read>
                </t:EffectiveRights>
              </t:Folder>
            </t:Folders>
          </m:RootFolder>
        </m:FindFolderResponseMessage>
      </m:ResponseMessages>
    </m:FindFolderResponse>
  </s:Body>
</s:Envelope>"#;
        let folders = parse_find_folder_response(xml).expect("parse should succeed");
        assert_eq!(folders.len(), 1);
        let rights = &folders[0].effective_rights;
        assert!(!rights.create_associated);
        assert!(!rights.create_contents);
        assert!(!rights.create_hierarchy);
        assert!(!rights.delete);
        assert!(!rights.modify);
        assert!(rights.read);
    }

    #[test]
    fn parse_find_items_response_two_items() {
        let xml = r#"<?xml version="1.0" encoding="utf-8"?>
<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/">
  <s:Body>
    <m:FindItemResponse xmlns:m="http://schemas.microsoft.com/exchange/services/2006/messages"
                        xmlns:t="http://schemas.microsoft.com/exchange/services/2006/types">
      <m:ResponseMessages>
        <m:FindItemResponseMessage ResponseClass="Success">
          <m:ResponseCode>NoError</m:ResponseCode>
          <m:RootFolder TotalItemsInView="150" IncludesLastItemInRange="false">
            <t:Items>
              <t:Message>
                <t:ItemId Id="AAMkItem1=" ChangeKey="CK1"/>
                <t:Subject>Q1 Results</t:Subject>
                <t:DateTimeReceived>2026-03-01T10:30:00Z</t:DateTimeReceived>
                <t:From>
                  <t:Mailbox>
                    <t:Name>Jane Doe</t:Name>
                    <t:EmailAddress>jane@contoso.com</t:EmailAddress>
                  </t:Mailbox>
                </t:From>
                <t:IsRead>true</t:IsRead>
                <t:Preview>Here are the Q1 financial results...</t:Preview>
                <t:ItemClass>IPM.Note</t:ItemClass>
              </t:Message>
              <t:Message>
                <t:ItemId Id="AAMkItem2=" ChangeKey="CK2"/>
                <t:Subject>Office Move Update</t:Subject>
                <t:DateTimeReceived>2026-02-28T14:15:00Z</t:DateTimeReceived>
                <t:From>
                  <t:Mailbox>
                    <t:Name>Facilities</t:Name>
                    <t:EmailAddress>facilities@contoso.com</t:EmailAddress>
                  </t:Mailbox>
                </t:From>
                <t:IsRead>false</t:IsRead>
                <t:Preview>The office move has been rescheduled...</t:Preview>
                <t:ItemClass>IPM.Note</t:ItemClass>
              </t:Message>
            </t:Items>
          </m:RootFolder>
        </m:FindItemResponseMessage>
      </m:ResponseMessages>
    </m:FindItemResponse>
  </s:Body>
</s:Envelope>"#;
        let result = parse_find_items_response(xml).expect("parse should succeed");
        assert_eq!(result.total_count, 150);
        assert!(!result.includes_last);
        assert_eq!(result.items.len(), 2);
        assert_eq!(result.items[0].item_id, "AAMkItem1=");
        assert_eq!(result.items[0].change_key.as_deref(), Some("CK1"));
        assert_eq!(result.items[0].subject.as_deref(), Some("Q1 Results"));
        assert_eq!(
            result.items[0].sender_email.as_deref(),
            Some("jane@contoso.com")
        );
        assert_eq!(result.items[0].sender_name.as_deref(), Some("Jane Doe"));
        assert!(result.items[0].is_read);
        assert_eq!(result.items[1].item_id, "AAMkItem2=");
        assert!(!result.items[1].is_read);
        assert_eq!(
            result.items[1].received_at.as_deref(),
            Some("2026-02-28T14:15:00Z")
        );
    }

    #[test]
    fn parse_find_items_response_empty() {
        let xml = r#"<?xml version="1.0" encoding="utf-8"?>
<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/">
  <s:Body>
    <m:FindItemResponse xmlns:m="http://schemas.microsoft.com/exchange/services/2006/messages"
                        xmlns:t="http://schemas.microsoft.com/exchange/services/2006/types">
      <m:ResponseMessages>
        <m:FindItemResponseMessage ResponseClass="Success">
          <m:ResponseCode>NoError</m:ResponseCode>
          <m:RootFolder TotalItemsInView="0" IncludesLastItemInRange="true">
            <t:Items/>
          </m:RootFolder>
        </m:FindItemResponseMessage>
      </m:ResponseMessages>
    </m:FindItemResponse>
  </s:Body>
</s:Envelope>"#;
        let result = parse_find_items_response(xml).expect("parse should succeed");
        assert_eq!(result.total_count, 0);
        assert!(result.includes_last);
        assert!(result.items.is_empty());
    }

    #[test]
    fn parse_get_item_response_full() {
        let xml = r#"<?xml version="1.0" encoding="utf-8"?>
<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/">
  <s:Body>
    <m:GetItemResponse xmlns:m="http://schemas.microsoft.com/exchange/services/2006/messages"
                       xmlns:t="http://schemas.microsoft.com/exchange/services/2006/types">
      <m:ResponseMessages>
        <m:GetItemResponseMessage ResponseClass="Success">
          <m:ResponseCode>NoError</m:ResponseCode>
          <m:Items>
            <t:Message>
              <t:ItemId Id="AAMkFull=" ChangeKey="CKFull"/>
              <t:Subject>Quarterly Review</t:Subject>
              <t:DateTimeReceived>2026-03-10T09:00:00Z</t:DateTimeReceived>
              <t:Body BodyType="HTML">&lt;html&gt;&lt;body&gt;Meeting notes here&lt;/body&gt;&lt;/html&gt;</t:Body>
              <t:IsRead>true</t:IsRead>
              <t:ItemClass>IPM.Note</t:ItemClass>
              <t:From>
                <t:Mailbox>
                  <t:Name>Alice Smith</t:Name>
                  <t:EmailAddress>alice@contoso.com</t:EmailAddress>
                </t:Mailbox>
              </t:From>
              <t:ToRecipients>
                <t:Mailbox>
                  <t:Name>Bob Jones</t:Name>
                  <t:EmailAddress>bob@contoso.com</t:EmailAddress>
                </t:Mailbox>
                <t:Mailbox>
                  <t:Name>Carol White</t:Name>
                  <t:EmailAddress>carol@contoso.com</t:EmailAddress>
                </t:Mailbox>
              </t:ToRecipients>
              <t:CcRecipients>
                <t:Mailbox>
                  <t:Name>Dave Brown</t:Name>
                  <t:EmailAddress>dave@contoso.com</t:EmailAddress>
                </t:Mailbox>
              </t:CcRecipients>
            </t:Message>
          </m:Items>
        </m:GetItemResponseMessage>
      </m:ResponseMessages>
    </m:GetItemResponse>
  </s:Body>
</s:Envelope>"#;
        let item = parse_get_item_response(xml).expect("parse should succeed");
        assert_eq!(item.item_id, "AAMkFull=");
        assert_eq!(item.change_key.as_deref(), Some("CKFull"));
        assert_eq!(item.subject.as_deref(), Some("Quarterly Review"));
        assert_eq!(item.sender_email.as_deref(), Some("alice@contoso.com"));
        assert_eq!(item.sender_name.as_deref(), Some("Alice Smith"));
        assert!(item.is_read);
        assert_eq!(item.item_class, "IPM.Note");
        assert!(item.body_html.is_some());
        assert!(item.body_html.as_deref().unwrap_or("").contains("<html>"));
        assert_eq!(item.to_recipients.len(), 2);
        assert_eq!(item.to_recipients[0].email, "bob@contoso.com");
        assert_eq!(item.to_recipients[0].name.as_deref(), Some("Bob Jones"));
        assert_eq!(item.to_recipients[1].email, "carol@contoso.com");
        assert_eq!(item.cc_recipients.len(), 1);
        assert_eq!(item.cc_recipients[0].email, "dave@contoso.com");
    }

    #[test]
    fn parse_get_item_response_empty_is_err() {
        let xml = r#"<?xml version="1.0" encoding="utf-8"?>
<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/">
  <s:Body>
    <m:GetItemResponse xmlns:m="http://schemas.microsoft.com/exchange/services/2006/messages">
      <m:ResponseMessages>
        <m:GetItemResponseMessage ResponseClass="Success">
          <m:ResponseCode>NoError</m:ResponseCode>
          <m:Items/>
        </m:GetItemResponseMessage>
      </m:ResponseMessages>
    </m:GetItemResponse>
  </s:Body>
</s:Envelope>"#;
        let result = parse_get_item_response(xml);
        assert!(matches!(result, Err(EwsError::MalformedXml(_))));
    }

    #[test]
    fn parse_get_folder_response_with_replica_list() {
        let guid = "{ABCD1234-EF56-7890-AB12-CDEF34567890}";
        let mut raw = Vec::new();
        raw.extend_from_slice(guid.as_bytes());
        raw.push(0);
        let b64 = BASE64.encode(&raw);

        let xml = format!(
            r#"<?xml version="1.0" encoding="utf-8"?>
<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/">
  <s:Body>
    <m:GetFolderResponse xmlns:m="http://schemas.microsoft.com/exchange/services/2006/messages"
                         xmlns:t="http://schemas.microsoft.com/exchange/services/2006/types">
      <m:ResponseMessages>
        <m:GetFolderResponseMessage ResponseClass="Success">
          <m:ResponseCode>NoError</m:ResponseCode>
          <m:Folders>
            <t:Folder>
              <t:FolderId Id="AAMkPF=" ChangeKey="CK1"/>
              <t:DisplayName>Public Docs</t:DisplayName>
              <t:TotalCount>55</t:TotalCount>
              <t:ChildFolderCount>2</t:ChildFolderCount>
              <t:UnreadCount>3</t:UnreadCount>
              <t:FolderClass>IPF.Note</t:FolderClass>
              <t:EffectiveRights>
                <t:CreateAssociated>false</t:CreateAssociated>
                <t:CreateContents>true</t:CreateContents>
                <t:CreateHierarchy>false</t:CreateHierarchy>
                <t:Delete>false</t:Delete>
                <t:Modify>false</t:Modify>
                <t:Read>true</t:Read>
              </t:EffectiveRights>
              <t:ExtendedProperty>
                <t:ExtendedFieldURI PropertyTag="0x6698" PropertyType="Binary"/>
                <t:Value>{b64}</t:Value>
              </t:ExtendedProperty>
            </t:Folder>
          </m:Folders>
        </m:GetFolderResponseMessage>
      </m:ResponseMessages>
    </m:GetFolderResponse>
  </s:Body>
</s:Envelope>"#
        );

        let folder = parse_get_folder_response(&xml).expect("parse should succeed");
        assert_eq!(folder.folder_id, "AAMkPF=");
        assert_eq!(folder.display_name, "Public Docs");
        assert_eq!(folder.total_count, 55);
        assert!(folder.replica_list.is_some());

        let replica_bytes = folder.replica_list.as_ref().expect("replica list present");
        let b64_round = BASE64.encode(replica_bytes);
        let guids = decode_replica_list(&b64_round).expect("decode should succeed");
        assert_eq!(guids.len(), 1);
        assert_eq!(guids[0], guid);
    }

    #[test]
    fn decode_replica_list_round_trip() {
        let guid1 = "{1A2B3C4D-5E6F-7A8B-9C0D-1E2F3A4B5C6D}";
        let guid2 = "{AAAABBBB-CCCC-DDDD-EEEE-FFFF00001111}";
        let mut raw = Vec::new();
        raw.extend_from_slice(guid1.as_bytes());
        raw.push(0);
        raw.extend_from_slice(guid2.as_bytes());
        raw.push(0);

        let encoded = BASE64.encode(&raw);
        let guids = decode_replica_list(&encoded).expect("decode should succeed");
        assert_eq!(guids.len(), 2);
        assert_eq!(guids[0], guid1);
        assert_eq!(guids[1], guid2);

        let empty = decode_replica_list(&BASE64.encode(b"")).expect("decode empty");
        assert!(empty.is_empty());
    }
}
