//! Exchange Autodiscover layer: public-folder routing discovery and
//! delegate (alternative-mailbox) enumeration.
//!
//! The two response parsers are copy-direct from ratatoskr's
//! `autodiscover.rs` (pure quick-xml, fully tested). The HTTP entry
//! points are reshaped onto `AccountNet` (the Bearer is supplied by the
//! net layer, never a hand-built header) and return `AccountError`
//! through the existing REST error path - Autodiscover is REST-over-HTTP
//! (SOAP body, but classified by HTTP status), not SOAP-faulting EWS, so
//! it does not route through `ews_error_to_account_error`.

use bifrost_net::error::cap_status_body;
use bifrost_types::{AccountError, AccountOperation};
use quick_xml::Reader;
use quick_xml::escape::unescape;
use quick_xml::events::Event;
use reqwest::header::HeaderMap;

use super::GraphAccount;
use super::cursor::PublicFolderRouting;
use super::graph_error::{GraphErrorContext, into_account_error, response_to_account_error_pub};
use crate::error::{GraphError, GraphResponseError};
use crate::ews::push_general_ref;

/// The POX (`autodiscover.xml`) Autodiscover endpoint under a given Outlook
/// origin. Derived rather than hardcoded so the harness api-base override
/// reaches Autodiscover too: production Autodiscover lives on
/// `outlook.office365.com`, not on the Graph host, so redirecting only the
/// Graph base left delegate/public-folder discovery hitting the real
/// service.
pub(crate) fn autodiscover_xml_url(outlook_base: &str) -> String {
    format!(
        "{}/autodiscover/autodiscover.xml",
        outlook_base.trim_end_matches('/')
    )
}

/// The SOAP (`autodiscover.svc` / `GetUserSettings`) Autodiscover endpoint
/// under a given Outlook origin. See [`autodiscover_xml_url`].
pub(crate) fn autodiscover_soap_url(outlook_base: &str) -> String {
    format!(
        "{}/autodiscover/autodiscover.svc",
        outlook_base.trim_end_matches('/')
    )
}

/// A shared/delegate mailbox discovered via Exchange Autodiscover.
/// Routing keys on `smtp_address` alone; `display_name` and
/// `mailbox_type` are parsed for wire fidelity (and asserted by the
/// parser tests) but the seeding path deliberately does not filter on
/// type, so they are unread in non-test builds.
#[derive(Debug, Clone)]
pub(crate) struct SharedMailbox {
    pub(crate) smtp_address: String,
    #[allow(dead_code)]
    pub(crate) display_name: Option<String>,
    /// E.g. "Delegate", "TeamMailbox", etc.
    #[allow(dead_code)]
    pub(crate) mailbox_type: String,
}

/// Construct a synthetic SMTP address from a `PR_REPLICA_LIST` GUID and
/// domain, the address Autodiscover resolves to the real content mailbox.
pub(crate) fn construct_replica_smtp(guid: &str, domain: &str) -> String {
    format!("{guid}@{domain}")
}

/// Reduce the `GetUserSettings` name/value pairs into a
/// `PublicFolderRouting`: `PublicFolderInformation` is the hierarchy
/// (anchor) mailbox, `InternalRpcClientServer` is the public-folder
/// mailbox server. Errors (returns `None`) when `PublicFolderInformation`
/// is absent - there is no usable routing without it.
pub(crate) fn public_folder_routing_from_settings(
    settings: &[(String, String)],
) -> Option<PublicFolderRouting> {
    let mut anchor_mailbox: Option<String> = None;
    let mut public_folder_mailbox: Option<String> = None;
    for (name, value) in settings {
        match name.as_str() {
            "PublicFolderInformation" => anchor_mailbox = Some(value.clone()),
            "InternalRpcClientServer" => public_folder_mailbox = Some(value.clone()),
            _ => {}
        }
    }
    Some(PublicFolderRouting {
        anchor_mailbox: anchor_mailbox?,
        public_folder_mailbox,
    })
}

impl GraphAccount {
    /// `GetUserSettings` SOAP for `PublicFolderInformation` +
    /// `InternalRpcClientServer` -> the hierarchy routing.
    pub(crate) async fn discover_public_folder_routing(
        &self,
        user_email: &str,
    ) -> Result<PublicFolderRouting, AccountError> {
        let settings = self
            .soap_get_user_settings(
                user_email,
                &["PublicFolderInformation", "InternalRpcClientServer"],
            )
            .await?;
        public_folder_routing_from_settings(&settings).ok_or_else(|| {
            super::graph_error::invalid_account_error(
                AccountOperation::Discover,
                "Autodiscover response missing PublicFolderInformation",
            )
        })
    }

    /// `GetUserSettings` for `AutoDiscoverSMTPAddress` -> the real
    /// content mailbox SMTP for a replica GUID's synthetic address.
    pub(crate) async fn discover_content_mailbox(
        &self,
        replica_smtp: &str,
    ) -> Result<String, AccountError> {
        let settings = self
            .soap_get_user_settings(replica_smtp, &["AutoDiscoverSMTPAddress"])
            .await?;
        settings
            .into_iter()
            .find_map(|(name, value)| (name == "AutoDiscoverSMTPAddress").then_some(value))
            .ok_or_else(|| {
                super::graph_error::invalid_account_error(
                    AccountOperation::Discover,
                    "Autodiscover response missing AutoDiscoverSMTPAddress",
                )
            })
    }

    /// `alternativeMailboxes` Autodiscover XML -> delegate mailbox list.
    /// Consumed by delegate auto-discovery at `open` when the factory's
    /// `with_delegate_discovery()` flag is set.
    ///
    /// Best-effort by design: `parse_alternative_mailboxes` treats
    /// malformed or truncated XML as end-of-input rather than an error
    /// (it is the shared quick-xml walking pattern used across this
    /// module), so an HTTP-200 response with bad XML yields an empty or
    /// partial mailbox list here, not an `Err`. That is non-fatal at the
    /// call site: `open` degrades to the config-supplied mailboxes when
    /// discovery comes back empty or fails outright.
    pub(crate) async fn discover_shared_mailboxes(
        &self,
        user_email: &str,
    ) -> Result<Vec<SharedMailbox>, AccountError> {
        let escaped_email = quick_xml::escape::escape(user_email);
        let body = format!(
            r#"<?xml version="1.0" encoding="utf-8"?>
<Autodiscover xmlns="http://schemas.microsoft.com/exchange/autodiscover/outlook/requestschema/2006">
  <Request>
    <EMailAddress>{escaped_email}</EMailAddress>
    <AcceptableResponseSchema>http://schemas.microsoft.com/exchange/autodiscover/outlook/responseschema/2006a</AcceptableResponseSchema>
  </Request>
</Autodiscover>"#
        );
        let url = autodiscover_xml_url(self.client.outlook_base());
        let xml = self.autodiscover_post(&url, "text/xml", None, body).await?;
        Ok(parse_alternative_mailboxes(&xml))
    }

    async fn soap_get_user_settings(
        &self,
        email: &str,
        settings: &[&str],
    ) -> Result<Vec<(String, String)>, AccountError> {
        // Autodiscover redirects (`RedirectAddr` to a new email,
        // `RedirectUrl` to a new endpoint) are common for hybrid /
        // on-prem tenants and ride in-body, not as an HTTP 3xx. Follow a
        // bounded chain; the cap guards against a redirect loop.
        const MAX_REDIRECTS: usize = 5;
        let mut url = autodiscover_soap_url(self.client.outlook_base());
        let mut email = email.to_string();

        for _ in 0..=MAX_REDIRECTS {
            let body = build_get_user_settings_soap(&email, settings);
            let xml = self
                .autodiscover_post(
                    &url,
                    "text/xml; charset=utf-8",
                    Some((
                        "SOAPAction",
                        "\"http://schemas.microsoft.com/exchange/2010/Autodiscover/Autodiscover/GetUserSettings\"",
                    )),
                    body,
                )
                .await?;
            let parsed = parse_user_settings_response(&xml);

            // A redirect target reroutes the lookup. `RedirectAddr`
            // (or any non-URL target) is a new mailbox to query against
            // the same endpoint; `RedirectUrl` is a new endpoint for the
            // same mailbox.
            if let Some(target) = parsed.redirect_target.as_deref() {
                if target.starts_with("http://") || target.starts_with("https://") {
                    url = target.to_string();
                } else {
                    email = target.to_string();
                }
                continue;
            }

            // An in-body error other than `NoError` is a real failure,
            // not an empty result. Surface it classified rather than
            // returning an empty settings vec the caller misreads as
            // "missing PublicFolderInformation".
            if let Some(code) = parsed
                .error_code
                .as_deref()
                .filter(|c| !c.is_empty() && !c.eq_ignore_ascii_case("NoError"))
            {
                let detail = parsed.error_message.as_deref().unwrap_or("");
                return Err(super::graph_error::invalid_account_error(
                    AccountOperation::Discover,
                    format!("Autodiscover GetUserSettings error {code}: {detail}"),
                ));
            }

            return Ok(parsed.settings);
        }

        Err(super::graph_error::invalid_account_error(
            AccountOperation::Discover,
            "Autodiscover GetUserSettings exceeded redirect limit",
        ))
    }

    /// Shared transport for both Autodiscover endpoints. Routes the
    /// Bearer through `AccountNet`; classifies failures through the REST
    /// error path with `Protocol::Graph`.
    async fn autodiscover_post(
        &self,
        url: &str,
        content_type: &str,
        extra_header: Option<(&str, &str)>,
        body: String,
    ) -> Result<String, AccountError> {
        let ctx = GraphErrorContext::graph(AccountOperation::Discover);
        let account_net = self.client.account_net().ok_or_else(|| {
            into_account_error(
                GraphError::Net(bifrost_net::Error::Network {
                    message: "Graph client is not attached to an account".to_string(),
                    transmission_state: bifrost_types::TransmissionState::Unsent,
                    source: None,
                }),
                ctx.clone(),
            )
        })?;

        let mut req = account_net.post(url).header("Content-Type", content_type);
        if let Some((name, value)) = extra_header {
            req = req.header(name, value);
        }
        let resp = req
            .body(bytes::Bytes::from(body))
            .send()
            .await
            .map_err(|error| into_account_error(GraphError::Net(error), ctx.clone()))?;

        let status = resp.status();
        if !status.is_success() {
            let response = GraphResponseError::from_response(
                status,
                HeaderMap::new(),
                cap_status_body(resp.body),
            );
            return Err(response_to_account_error_pub(response, &ctx));
        }
        Ok(String::from_utf8_lossy(resp.body.as_ref()).into_owned())
    }
}

fn build_get_user_settings_soap(email: &str, settings: &[&str]) -> String {
    let escaped_email = quick_xml::escape::escape(email);
    let settings_xml: String = settings
        .iter()
        .map(|s| format!("          <a:Setting>{s}</a:Setting>"))
        .collect::<Vec<_>>()
        .join("\n");
    format!(
        r#"<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/"
               xmlns:a="http://schemas.microsoft.com/exchange/2010/Autodiscover">
  <soap:Header>
    <a:RequestedServerVersion>Exchange2016</a:RequestedServerVersion>
  </soap:Header>
  <soap:Body>
    <a:GetUserSettingsRequestMessage>
      <a:Request>
        <a:Users>
          <a:User>
            <a:Mailbox>{escaped_email}</a:Mailbox>
          </a:User>
        </a:Users>
        <a:RequestedSettings>
{settings_xml}
        </a:RequestedSettings>
      </a:Request>
    </a:GetUserSettingsRequestMessage>
  </soap:Body>
</soap:Envelope>"#
    )
}

// ── Pure parsers (copy-direct from ratatoskr) ───────────────

/// The parsed shape of a `GetUserSettings` response. EWS Autodiscover
/// returns failures and redirects INSIDE an HTTP 200 (the in-body
/// `<a:ErrorCode>` / `<a:RedirectTarget>`), so a status-only check reads
/// a failed or redirecting response as an empty settings vec. This
/// captures all three so the caller can act on them.
#[derive(Debug, Default)]
pub(crate) struct UserSettingsResponse {
    pub(crate) settings: Vec<(String, String)>,
    /// In-body `<a:ErrorCode>` (`NoError` / empty when absent).
    pub(crate) error_code: Option<String>,
    pub(crate) error_message: Option<String>,
    /// In-body `<a:RedirectTarget>` (the SMTP address or URL to retry
    /// against; common for hybrid / on-prem tenants).
    pub(crate) redirect_target: Option<String>,
}

/// Parse `UserSetting` `<Name>`/`<Value>` pairs from a
/// `GetUserSettings` SOAP response. Thin wrapper over
/// `parse_user_settings_response` for the settings-only tests; the
/// production path consumes the full response (error/redirect markers).
#[cfg(test)]
fn parse_user_settings(xml: &str) -> Vec<(String, String)> {
    parse_user_settings_response(xml).settings
}

/// Parse a `GetUserSettings` response into settings plus the in-body
/// error / redirect markers.
fn parse_user_settings_response(xml: &str) -> UserSettingsResponse {
    let mut reader = Reader::from_str(xml);
    let mut out = UserSettingsResponse::default();

    let mut in_user_setting = false;
    let mut current_name = String::new();
    let mut current_value = String::new();
    let mut current_tag = String::new();
    let mut buf = String::new();

    loop {
        match reader.read_event() {
            Ok(Event::Start(ref e)) => {
                let name = String::from_utf8_lossy(e.name().local_name().as_ref()).to_string();
                if name == "UserSetting" {
                    in_user_setting = true;
                    current_name.clear();
                    current_value.clear();
                }
                current_tag = name;
                buf.clear();
            }
            Ok(Event::Text(ref e)) => push_text(e, &mut buf),
            Ok(Event::GeneralRef(ref e)) => push_general_ref(e, &mut buf),
            Ok(Event::End(ref e)) => {
                let name = String::from_utf8_lossy(e.name().local_name().as_ref()).to_string();
                let trimmed = buf.trim();
                if in_user_setting {
                    match current_tag.as_str() {
                        "Name" => current_name = trimmed.to_string(),
                        // A legitimately-empty setting Value is retained
                        // (a present-but-empty setting is not the same as
                        // an absent one).
                        "Value" => current_value = trimmed.to_string(),
                        _ => {}
                    }
                } else {
                    // Response-level (not per-UserSetting) markers.
                    match current_tag.as_str() {
                        "ErrorCode" if out.error_code.is_none() => {
                            out.error_code = Some(trimmed.to_string());
                        }
                        "ErrorMessage" if out.error_message.is_none() && !trimmed.is_empty() => {
                            out.error_message = Some(trimmed.to_string());
                        }
                        "RedirectTarget"
                            if out.redirect_target.is_none() && !trimmed.is_empty() =>
                        {
                            out.redirect_target = Some(trimmed.to_string());
                        }
                        _ => {}
                    }
                }
                if name == "UserSetting" {
                    in_user_setting = false;
                    // Keep the pair when a Name is present; an empty Value
                    // is a legitimate setting, not a reason to drop it.
                    if !current_name.is_empty() {
                        out.settings
                            .push((current_name.clone(), current_value.clone()));
                    }
                }
                buf.clear();
                current_tag.clear();
            }
            Ok(Event::Eof) | Err(_) => break,
            _ => {}
        }
    }

    out
}

/// Merge config-supplied shared-mailbox routing keys with the
/// Autodiscover-enumerated delegates into a single deduplicated list.
/// Config entries come first and win ordering; every entry in the merged
/// set (config first, discovered appended) is empty-dropped and deduped
/// by exact string uniformly - a blank config entry seeds no client, and
/// a delegate named both in config and by discovery (or discovered
/// twice) is not seeded twice. Every discovered alternative mailbox is
/// kept regardless of `mailbox_type` - the routing layer keys on the
/// address alone.
pub(crate) fn merge_shared_mailboxes(
    config: &[String],
    discovered: &[SharedMailbox],
) -> Vec<String> {
    let candidates = config.iter().cloned().chain(
        discovered
            .iter()
            .map(|mailbox| mailbox.smtp_address.clone()),
    );
    let mut merged: Vec<String> = Vec::new();
    for candidate in candidates {
        if !candidate.is_empty() && !merged.iter().any(|existing| existing == &candidate) {
            merged.push(candidate);
        }
    }
    merged
}

/// Parse `AlternativeMailbox` elements from an Autodiscover XML response.
fn parse_alternative_mailboxes(xml: &str) -> Vec<SharedMailbox> {
    let mut reader = Reader::from_str(xml);
    let mut mailboxes = Vec::new();

    let mut in_alternative_mailbox = false;
    let mut current_type = String::new();
    let mut current_display_name = String::new();
    let mut current_smtp = String::new();
    let mut current_tag = String::new();
    let mut buf = String::new();

    loop {
        match reader.read_event() {
            Ok(Event::Start(ref e)) => {
                let name = String::from_utf8_lossy(e.name().local_name().as_ref()).to_string();
                if name == "AlternativeMailbox" {
                    in_alternative_mailbox = true;
                    current_type.clear();
                    current_display_name.clear();
                    current_smtp.clear();
                }
                current_tag = name;
                buf.clear();
            }
            Ok(Event::Text(ref e)) => push_text(e, &mut buf),
            Ok(Event::GeneralRef(ref e)) => push_general_ref(e, &mut buf),
            Ok(Event::End(ref e)) => {
                let name = String::from_utf8_lossy(e.name().local_name().as_ref()).to_string();
                if in_alternative_mailbox {
                    let trimmed = buf.trim();
                    match current_tag.as_str() {
                        "Type" => current_type = trimmed.to_string(),
                        "DisplayName" => current_display_name = trimmed.to_string(),
                        "SmtpAddress" => current_smtp = trimmed.to_string(),
                        _ => {}
                    }
                }
                if name == "AlternativeMailbox" {
                    in_alternative_mailbox = false;
                    if !current_smtp.is_empty() {
                        mailboxes.push(SharedMailbox {
                            smtp_address: current_smtp.clone(),
                            display_name: if current_display_name.is_empty() {
                                None
                            } else {
                                Some(current_display_name.clone())
                            },
                            mailbox_type: current_type.clone(),
                        });
                    }
                }
                buf.clear();
                current_tag.clear();
            }
            Ok(Event::Eof) | Err(_) => break,
            _ => {}
        }
    }

    mailboxes
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
    fn parse_single_alternative_mailbox() {
        let xml = r#"<?xml version="1.0" encoding="utf-8"?>
<Autodiscover xmlns="http://schemas.microsoft.com/exchange/autodiscover/outlook/responseschema/2006a">
  <Response>
    <Account>
      <AlternativeMailbox>
        <Type>Delegate</Type>
        <DisplayName>Sales Team</DisplayName>
        <SmtpAddress>sales@contoso.com</SmtpAddress>
      </AlternativeMailbox>
    </Account>
  </Response>
</Autodiscover>"#;
        let result = parse_alternative_mailboxes(xml);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].smtp_address, "sales@contoso.com");
        assert_eq!(result[0].display_name.as_deref(), Some("Sales Team"));
        assert_eq!(result[0].mailbox_type, "Delegate");
    }

    #[test]
    fn parse_multiple_alternative_mailboxes() {
        let xml = r#"<?xml version="1.0" encoding="utf-8"?>
<Autodiscover>
  <Response>
    <Account>
      <AlternativeMailbox>
        <Type>Delegate</Type>
        <DisplayName>Sales Team</DisplayName>
        <SmtpAddress>sales@contoso.com</SmtpAddress>
      </AlternativeMailbox>
      <AlternativeMailbox>
        <Type>TeamMailbox</Type>
        <DisplayName>Engineering</DisplayName>
        <SmtpAddress>eng@contoso.com</SmtpAddress>
      </AlternativeMailbox>
      <AlternativeMailbox>
        <Type>Delegate</Type>
        <SmtpAddress>noreply@contoso.com</SmtpAddress>
      </AlternativeMailbox>
    </Account>
  </Response>
</Autodiscover>"#;
        let result = parse_alternative_mailboxes(xml);
        assert_eq!(result.len(), 3);
        assert_eq!(result[0].smtp_address, "sales@contoso.com");
        assert_eq!(result[0].mailbox_type, "Delegate");
        assert_eq!(result[1].smtp_address, "eng@contoso.com");
        assert_eq!(result[1].display_name.as_deref(), Some("Engineering"));
        assert_eq!(result[1].mailbox_type, "TeamMailbox");
        assert_eq!(result[2].smtp_address, "noreply@contoso.com");
        assert_eq!(result[2].display_name, None);
    }

    #[test]
    fn parse_alternative_mailboxes_empty() {
        let xml = r#"<?xml version="1.0" encoding="utf-8"?>
<Autodiscover>
  <Response>
    <Account>
    </Account>
  </Response>
</Autodiscover>"#;
        assert!(parse_alternative_mailboxes(xml).is_empty());
    }

    #[test]
    fn skip_mailbox_without_smtp_address() {
        let xml = r#"<Autodiscover>
  <Response>
    <Account>
      <AlternativeMailbox>
        <Type>Delegate</Type>
        <DisplayName>Broken Entry</DisplayName>
      </AlternativeMailbox>
    </Account>
  </Response>
</Autodiscover>"#;
        assert!(parse_alternative_mailboxes(xml).is_empty());
    }

    fn shared(smtp: &str, ty: &str) -> SharedMailbox {
        SharedMailbox {
            smtp_address: smtp.to_string(),
            display_name: None,
            mailbox_type: ty.to_string(),
        }
    }

    #[test]
    fn merge_appends_discovered_to_config() {
        let config = vec!["configured@contoso.com".to_string()];
        let discovered = vec![shared("sales@contoso.com", "Delegate")];
        let merged = merge_shared_mailboxes(&config, &discovered);
        assert_eq!(merged, vec!["configured@contoso.com", "sales@contoso.com"]);
    }

    #[test]
    fn merge_dedups_overlap_keeping_config_order() {
        let config = vec![
            "shared@contoso.com".to_string(),
            "eng@contoso.com".to_string(),
        ];
        // `shared@` is both config-supplied and discovered: it must not
        // seed a second client. `sales@` is new and appends.
        let discovered = vec![
            shared("shared@contoso.com", "Delegate"),
            shared("sales@contoso.com", "TeamMailbox"),
        ];
        let merged = merge_shared_mailboxes(&config, &discovered);
        assert_eq!(
            merged,
            vec!["shared@contoso.com", "eng@contoso.com", "sales@contoso.com"]
        );
    }

    #[test]
    fn merge_ignores_empty_smtp_and_keeps_all_types() {
        let config: Vec<String> = Vec::new();
        // An empty smtp_address is dropped; a non-Delegate type is kept
        // (no type filtering - the routing layer keys on the address).
        let discovered = vec![
            shared("", "Delegate"),
            shared("team@contoso.com", "TeamMailbox"),
        ];
        let merged = merge_shared_mailboxes(&config, &discovered);
        assert_eq!(merged, vec!["team@contoso.com"]);
    }

    #[test]
    fn merge_drops_empty_config_entries() {
        // A blank `with_shared_mailbox("")` entry must not seed a
        // `/users/` client with an empty routing key.
        let config = vec![
            String::new(),
            "configured@contoso.com".to_string(),
            String::new(),
        ];
        let discovered: Vec<SharedMailbox> = Vec::new();
        let merged = merge_shared_mailboxes(&config, &discovered);
        assert_eq!(merged, vec!["configured@contoso.com"]);
    }

    #[test]
    fn merge_dedups_repeated_config_entries() {
        let config = vec![
            "configured@contoso.com".to_string(),
            "configured@contoso.com".to_string(),
        ];
        let discovered = vec![shared("sales@contoso.com", "Delegate")];
        let merged = merge_shared_mailboxes(&config, &discovered);
        assert_eq!(merged, vec!["configured@contoso.com", "sales@contoso.com"]);
    }

    #[test]
    fn merge_dedups_repeated_discovered_entries() {
        let config: Vec<String> = Vec::new();
        let discovered = vec![
            shared("dup@contoso.com", "Delegate"),
            shared("dup@contoso.com", "Delegate"),
        ];
        let merged = merge_shared_mailboxes(&config, &discovered);
        assert_eq!(merged, vec!["dup@contoso.com"]);
    }

    #[test]
    fn parse_public_folder_routing_settings() {
        let xml = r#"<?xml version="1.0" encoding="utf-8"?>
<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/"
            xmlns:a="http://schemas.microsoft.com/exchange/2010/Autodiscover">
  <s:Body>
    <a:GetUserSettingsResponseMessage>
      <a:Response>
        <a:UserResponses>
          <a:UserResponse>
            <a:UserSettings>
              <a:UserSetting>
                <a:Name>PublicFolderInformation</a:Name>
                <a:Value>publicfolders@contoso.com</a:Value>
              </a:UserSetting>
              <a:UserSetting>
                <a:Name>InternalRpcClientServer</a:Name>
                <a:Value>server01.contoso.com</a:Value>
              </a:UserSetting>
            </a:UserSettings>
          </a:UserResponse>
        </a:UserResponses>
      </a:Response>
    </a:GetUserSettingsResponseMessage>
  </s:Body>
</s:Envelope>"#;
        let settings = parse_user_settings(xml);
        assert_eq!(settings.len(), 2);
        assert_eq!(settings[0].0, "PublicFolderInformation");
        assert_eq!(settings[0].1, "publicfolders@contoso.com");
        assert_eq!(settings[1].0, "InternalRpcClientServer");
        assert_eq!(settings[1].1, "server01.contoso.com");
    }

    #[test]
    fn parse_autodiscover_smtp_address() {
        let xml = r#"<?xml version="1.0" encoding="utf-8"?>
<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/"
            xmlns:a="http://schemas.microsoft.com/exchange/2010/Autodiscover">
  <s:Body>
    <a:GetUserSettingsResponseMessage>
      <a:Response>
        <a:UserResponses>
          <a:UserResponse>
            <a:UserSettings>
              <a:UserSetting>
                <a:Name>AutoDiscoverSMTPAddress</a:Name>
                <a:Value>contentmailbox@contoso.com</a:Value>
              </a:UserSetting>
            </a:UserSettings>
          </a:UserResponse>
        </a:UserResponses>
      </a:Response>
    </a:GetUserSettingsResponseMessage>
  </s:Body>
</s:Envelope>"#;
        let settings = parse_user_settings(xml);
        assert_eq!(settings.len(), 1);
        assert_eq!(settings[0].0, "AutoDiscoverSMTPAddress");
        assert_eq!(settings[0].1, "contentmailbox@contoso.com");
    }

    #[test]
    fn parse_user_settings_empty() {
        let xml = r#"<?xml version="1.0" encoding="utf-8"?>
<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/">
  <s:Body>
    <GetUserSettingsResponseMessage>
      <Response>
        <UserResponses>
          <UserResponse>
            <UserSettings />
          </UserResponse>
        </UserResponses>
      </Response>
    </GetUserSettingsResponseMessage>
  </s:Body>
</s:Envelope>"#;
        assert!(parse_user_settings(xml).is_empty());
    }

    #[test]
    fn user_settings_in_body_error_is_captured() {
        let xml = r#"<?xml version="1.0" encoding="utf-8"?>
<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/"
            xmlns:a="http://schemas.microsoft.com/exchange/2010/Autodiscover">
  <s:Body>
    <a:GetUserSettingsResponseMessage>
      <a:Response>
        <a:ErrorCode>InvalidUser</a:ErrorCode>
        <a:ErrorMessage>The user could not be found.</a:ErrorMessage>
        <a:UserResponses>
          <a:UserResponse>
            <a:UserSettings/>
          </a:UserResponse>
        </a:UserResponses>
      </a:Response>
    </a:GetUserSettingsResponseMessage>
  </s:Body>
</s:Envelope>"#;
        let parsed = parse_user_settings_response(xml);
        assert!(parsed.settings.is_empty());
        assert_eq!(parsed.error_code.as_deref(), Some("InvalidUser"));
        assert_eq!(
            parsed.error_message.as_deref(),
            Some("The user could not be found.")
        );
    }

    #[test]
    fn user_settings_redirect_target_is_captured() {
        let xml = r#"<?xml version="1.0" encoding="utf-8"?>
<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/"
            xmlns:a="http://schemas.microsoft.com/exchange/2010/Autodiscover">
  <s:Body>
    <a:GetUserSettingsResponseMessage>
      <a:Response>
        <a:ErrorCode>RedirectAddress</a:ErrorCode>
        <a:RedirectTarget>user@redirected.contoso.com</a:RedirectTarget>
      </a:Response>
    </a:GetUserSettingsResponseMessage>
  </s:Body>
</s:Envelope>"#;
        let parsed = parse_user_settings_response(xml);
        assert_eq!(
            parsed.redirect_target.as_deref(),
            Some("user@redirected.contoso.com")
        );
    }

    #[test]
    fn user_settings_retains_empty_value() {
        let xml = r#"<?xml version="1.0" encoding="utf-8"?>
<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/"
            xmlns:a="http://schemas.microsoft.com/exchange/2010/Autodiscover">
  <s:Body>
    <a:GetUserSettingsResponseMessage>
      <a:Response>
        <a:ErrorCode>NoError</a:ErrorCode>
        <a:UserResponses>
          <a:UserResponse>
            <a:UserSettings>
              <a:UserSetting>
                <a:Name>PresentButEmpty</a:Name>
                <a:Value></a:Value>
              </a:UserSetting>
            </a:UserSettings>
          </a:UserResponse>
        </a:UserResponses>
      </a:Response>
    </a:GetUserSettingsResponseMessage>
  </s:Body>
</s:Envelope>"#;
        let parsed = parse_user_settings_response(xml);
        assert_eq!(parsed.settings.len(), 1);
        assert_eq!(parsed.settings[0].0, "PresentButEmpty");
        assert_eq!(parsed.settings[0].1, "");
    }

    #[test]
    fn construct_replica_smtp_helper() {
        assert_eq!(
            construct_replica_smtp("1A2B3C4D-5E6F-7A8B-9C0D-1E2F3A4B5C6D", "contoso.com"),
            "1A2B3C4D-5E6F-7A8B-9C0D-1E2F3A4B5C6D@contoso.com"
        );
    }

    #[test]
    fn public_folder_routing_from_settings_maps_fields() {
        let settings = vec![
            (
                "PublicFolderInformation".to_string(),
                "publicfolders@contoso.com".to_string(),
            ),
            (
                "InternalRpcClientServer".to_string(),
                "server01.contoso.com".to_string(),
            ),
        ];
        let routing = public_folder_routing_from_settings(&settings).expect("routing");
        assert_eq!(routing.anchor_mailbox, "publicfolders@contoso.com");
        assert_eq!(
            routing.public_folder_mailbox.as_deref(),
            Some("server01.contoso.com")
        );

        // Absent PublicFolderInformation -> None.
        let only_server = vec![(
            "InternalRpcClientServer".to_string(),
            "server01.contoso.com".to_string(),
        )];
        assert!(public_folder_routing_from_settings(&only_server).is_none());
    }
}
