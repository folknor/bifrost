//! EWS read operations: `FindFolder`, `GetFolder`, `FindItem`,
//! `GetItem`. SOAP bodies are the ratatoskr request bodies verbatim;
//! the only reshape is the error boundary (`EwsError` not `String`) and
//! the transport (`AccountNet`, with the Bearer supplied by the net
//! layer, never a hand-built header). Routing headers thread through
//! every call via `EwsHeaders`.
//!
//! The pure body builders are factored out so their shape (the
//! `DateTimeReceived` restriction, distinguished-vs-opaque folder id) is
//! unit-pinnable without a live request.

use super::xml_helpers::{is_distinguished_folder_id, xml_escape};
use super::{EwsClient, EwsError, EwsFolder, EwsHeaders, EwsItem, FindItemsResult};
use super::{
    parse_find_folder_response, parse_find_items_response, parse_get_folder_response,
    parse_get_item_response,
};

impl EwsClient {
    /// Browse child folders under a parent. Pass `"publicfoldersroot"`
    /// for the top of the public-folder hierarchy.
    pub(crate) async fn find_folder(
        &self,
        parent_folder_id: &str,
        headers: &EwsHeaders,
    ) -> Result<Vec<EwsFolder>, EwsError> {
        let body = find_folder_body(parent_folder_id);
        let xml = self.execute(&body, headers).await?;
        parse_find_folder_response(&xml)
    }

    /// Fetch detailed info for one folder, including `PR_REPLICA_LIST`
    /// (used to resolve the content-mailbox routing for a public
    /// folder).
    pub(crate) async fn get_folder(
        &self,
        folder_id: &str,
        headers: &EwsHeaders,
    ) -> Result<EwsFolder, EwsError> {
        let body = get_folder_body(folder_id);
        let xml = self.execute(&body, headers).await?;
        parse_get_folder_response(&xml)
    }

    /// Find items in a folder, paged, optionally restricted to those
    /// with `DateTimeReceived >= since`, sorted descending by received
    /// time.
    pub(crate) async fn find_items(
        &self,
        folder_id: &str,
        since: Option<&str>,
        offset: u32,
        max_entries: u32,
        headers: &EwsHeaders,
    ) -> Result<FindItemsResult, EwsError> {
        let body = find_items_body(folder_id, since, offset, max_entries);
        let xml = self.execute(&body, headers).await?;
        parse_find_items_response(&xml)
    }

    /// Fetch the full body + recipients of a single item. Part of the
    /// read-ops foundation; the poll/inventory paths sync via
    /// `find_items` (IdOnly), so `get_item` is unwired - consumed by
    /// per-item hydration once that path lands.
    ///
    /// The request body is message-shaped (`get_item_body` asks for
    /// `message:ToRecipients`/`CcRecipients`), so it is only class-safe for
    /// `<t:Message>`: a real Contact/CalendarItem GetItem would return
    /// `ErrorInvalidPropertyRequest`. Making the shape class-conditional is
    /// deferred with the hydration wiring - `get_item` takes only an id, so
    /// the class is not even known here yet. The parser tolerates all three
    /// classes (see `parse_get_item_response`); that tolerance is not a
    /// claim of operational non-mail GetItem support.
    #[allow(dead_code)]
    pub(crate) async fn get_item(
        &self,
        item_id: &str,
        headers: &EwsHeaders,
    ) -> Result<EwsItem, EwsError> {
        let body = get_item_body(item_id);
        let xml = self.execute(&body, headers).await?;
        parse_get_item_response(&xml)
    }
}

// ── SOAP body builders (pure) ───────────────────────────────

fn find_folder_body(parent_folder_id: &str) -> String {
    let escaped_id = xml_escape(parent_folder_id);
    let parent_xml = if is_distinguished_folder_id(parent_folder_id) {
        format!(r#"<t:DistinguishedFolderId Id="{escaped_id}"/>"#)
    } else {
        format!(r#"<t:FolderId Id="{escaped_id}"/>"#)
    };
    format!(
        r#"<m:FindFolder Traversal="Shallow">
  <m:FolderShape>
    <t:BaseShape>Default</t:BaseShape>
    <t:AdditionalProperties>
      <t:FieldURI FieldURI="folder:EffectiveRights"/>
      <t:FieldURI FieldURI="folder:FolderClass"/>
    </t:AdditionalProperties>
  </m:FolderShape>
  <m:ParentFolderIds>
    {parent_xml}
  </m:ParentFolderIds>
</m:FindFolder>"#
    )
}

fn get_folder_body(folder_id: &str) -> String {
    let escaped_id = xml_escape(folder_id);
    format!(
        r#"<m:GetFolder>
  <m:FolderShape>
    <t:BaseShape>Default</t:BaseShape>
    <t:AdditionalProperties>
      <t:FieldURI FieldURI="folder:EffectiveRights"/>
      <t:FieldURI FieldURI="folder:FolderClass"/>
      <t:ExtendedFieldURI PropertyTag="0x6698" PropertyType="Binary"/>
    </t:AdditionalProperties>
  </m:FolderShape>
  <m:FolderIds>
    <t:FolderId Id="{escaped_id}"/>
  </m:FolderIds>
</m:GetFolder>"#
    )
}

fn find_items_body(folder_id: &str, since: Option<&str>, offset: u32, max_entries: u32) -> String {
    let restriction = match since {
        Some(dt) => {
            let escaped_dt = xml_escape(dt);
            format!(
                r#"<m:Restriction>
    <t:IsGreaterThanOrEqualTo>
      <t:FieldURI FieldURI="item:DateTimeReceived"/>
      <t:FieldURIOrConstant>
        <t:Constant Value="{escaped_dt}"/>
      </t:FieldURIOrConstant>
    </t:IsGreaterThanOrEqualTo>
  </m:Restriction>"#
            )
        }
        None => String::new(),
    };
    let escaped_folder_id = xml_escape(folder_id);
    format!(
        r#"<m:FindItem Traversal="Shallow">
  <m:ItemShape>
    <t:BaseShape>IdOnly</t:BaseShape>
    <t:AdditionalProperties>
      <t:FieldURI FieldURI="item:Subject"/>
      <t:FieldURI FieldURI="item:DateTimeReceived"/>
      <t:FieldURI FieldURI="message:From"/>
      <t:FieldURI FieldURI="item:Preview"/>
      <t:FieldURI FieldURI="message:IsRead"/>
      <t:FieldURI FieldURI="item:ItemClass"/>
    </t:AdditionalProperties>
  </m:ItemShape>
  <m:IndexedPageItemView MaxEntriesReturned="{max_entries}" Offset="{offset}" BasePoint="Beginning"/>
  {restriction}
  <m:SortOrder>
    <t:FieldOrder Order="Descending">
      <t:FieldURI FieldURI="item:DateTimeReceived"/>
    </t:FieldOrder>
  </m:SortOrder>
  <m:ParentFolderIds>
    <t:FolderId Id="{escaped_folder_id}"/>
  </m:ParentFolderIds>
</m:FindItem>"#
    )
}

// Message-shaped: requests `message:ToRecipients`/`CcRecipients`, so this
// body is only class-safe for `<t:Message>`. Non-mail GetItem is unwired
// (sync runs through FindItem IdOnly); a class-conditional shape lands with
// the per-item hydration path. See the `get_item` doc comment.
#[allow(dead_code)]
fn get_item_body(item_id: &str) -> String {
    let escaped_id = xml_escape(item_id);
    format!(
        r#"<m:GetItem>
  <m:ItemShape>
    <t:BaseShape>Default</t:BaseShape>
    <t:AdditionalProperties>
      <t:FieldURI FieldURI="item:Body"/>
      <t:FieldURI FieldURI="item:Attachments"/>
      <t:FieldURI FieldURI="message:ToRecipients"/>
      <t:FieldURI FieldURI="message:CcRecipients"/>
    </t:AdditionalProperties>
    <t:BodyType>HTML</t:BodyType>
  </m:ItemShape>
  <m:ItemIds>
    <t:ItemId Id="{escaped_id}"/>
  </m:ItemIds>
</m:GetItem>"#
    )
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn find_items_body_includes_datetime_restriction() {
        let with = find_items_body("AAMk=", Some("2026-03-01T10:00:00Z"), 0, 50);
        assert!(with.contains("IsGreaterThanOrEqualTo"));
        assert!(with.contains(r#"FieldURI="item:DateTimeReceived""#));
        assert!(with.contains(r#"Value="2026-03-01T10:00:00Z""#));

        let without = find_items_body("AAMk=", None, 0, 50);
        assert!(!without.contains("IsGreaterThanOrEqualTo"));
        assert!(!without.contains("Restriction"));
        // The descending DateTimeReceived sort is present regardless.
        assert!(without.contains(r#"Order="Descending""#));
    }

    #[test]
    fn find_folder_distinguished_vs_id() {
        let distinguished = find_folder_body("publicfoldersroot");
        assert!(distinguished.contains(r#"<t:DistinguishedFolderId Id="publicfoldersroot"/>"#));
        assert!(!distinguished.contains("<t:FolderId"));

        let opaque = find_folder_body("AAMkAGFk=");
        assert!(opaque.contains(r#"<t:FolderId Id="AAMkAGFk="/>"#));
        assert!(!opaque.contains("DistinguishedFolderId"));
    }

    #[test]
    fn get_folder_body_requests_replica_list() {
        let body = get_folder_body("AAMkPF=");
        assert!(body.contains(r#"PropertyTag="0x6698""#));
        assert!(body.contains(r#"<t:FolderId Id="AAMkPF="/>"#));
    }

    #[test]
    fn routing_headers_pairs() {
        let both = EwsHeaders {
            anchor_mailbox: Some("anchor@contoso.com".to_string()),
            public_folder_mailbox: Some("pf@contoso.com".to_string()),
        };
        let pairs = both.pairs();
        assert_eq!(pairs.len(), 2);
        assert!(
            pairs
                .iter()
                .any(|(k, v)| *k == "X-AnchorMailbox" && v == "anchor@contoso.com")
        );
        assert!(
            pairs
                .iter()
                .any(|(k, v)| *k == "X-PublicFolderMailbox" && v == "pf@contoso.com")
        );

        let anchor_only = EwsHeaders {
            anchor_mailbox: Some("a@contoso.com".to_string()),
            public_folder_mailbox: None,
        };
        let pairs = anchor_only.pairs();
        assert_eq!(pairs.len(), 1);
        assert_eq!(pairs[0].0, "X-AnchorMailbox");

        assert!(EwsHeaders::default().pairs().is_empty());
    }
}
