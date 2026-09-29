//! Exchange Autodiscover layer: public-folder routing discovery and
//! delegate (alternative-mailbox) enumeration.
//!
//! The two response parsers derive from ratatoskr's `autodiscover.rs` (pure
//! quick-xml), since hardened to refuse a malformed or truncated document
//! rather than read it as an empty answer. The HTTP entry
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

use super::GraphAccount;
use super::cursor::PublicFolderRouting;
use super::graph_error::{GraphErrorContext, into_account_error, response_to_account_error_pub};
use crate::client::AuxTarget;
use crate::error::GraphResponseError;
use crate::ews::try_push_general_ref;
use crate::origin::AdmittedUrl;

/// The POX (`autodiscover.xml`) Autodiscover path under the Outlook origin.
/// Resolved against the client's Outlook base rather than hardcoded so the
/// harness api-base override reaches Autodiscover too: production
/// Autodiscover lives on `outlook.office365.com`, not on the Graph host, so
/// redirecting only the Graph base left delegate/public-folder discovery
/// hitting the real service.
const AUTODISCOVER_XML_PATH: &str = "/autodiscover/autodiscover.xml";

/// The SOAP (`autodiscover.svc` / `GetUserSettings`) Autodiscover path under
/// the Outlook origin. See [`AUTODISCOVER_XML_PATH`].
const AUTODISCOVER_SOAP_PATH: &str = "/autodiscover/autodiscover.svc";

const REDIRECT_ADDRESS: &str = "RedirectAddress";
const REDIRECT_URL: &str = "RedirectUrl";

/// The POX Autodiscover endpoint under a given Outlook origin.
#[cfg(test)]
pub(crate) fn autodiscover_xml_url(outlook_base: &str) -> String {
    format!(
        "{}{AUTODISCOVER_XML_PATH}",
        outlook_base.trim_end_matches('/')
    )
}

/// The SOAP Autodiscover endpoint under a given Outlook origin.
#[cfg(test)]
pub(crate) fn autodiscover_soap_url(outlook_base: &str) -> String {
    format!(
        "{}{AUTODISCOVER_SOAP_PATH}",
        outlook_base.trim_end_matches('/')
    )
}

/// What one `GetUserSettings` answer asks the lookup to do next.
enum SoapStep {
    /// The answer carries the settings.
    Settings,
    /// `RedirectAddress`: query this mailbox against the same endpoint.
    Mailbox(String),
    /// `RedirectUrl`: query the same mailbox at this admitted endpoint.
    Endpoint(AdmittedUrl),
}

/// The code, when it reports anything other than success.
fn failing_code(code: Option<&str>) -> Option<&str> {
    code.filter(|code| !code.is_empty() && !code.eq_ignore_ascii_case("NoError"))
}

fn is_redirect(code: &str) -> bool {
    code.eq_ignore_ascii_case(REDIRECT_ADDRESS) || code.eq_ignore_ascii_case(REDIRECT_URL)
}

/// Whether a redirect target is written as an http or https URL, judged by
/// the URL parser (so the scheme's case does not matter).
fn is_http_url(target: &str) -> bool {
    reqwest::Url::parse(target).is_ok_and(|url| matches!(url.scheme(), "http" | "https"))
}

/// An Autodiscover HTTP 200 whose body the provider got wrong: the
/// provider's malformed response (`Protocol(ParseFailed)`, `Acknowledged`).
/// The body is not attached: Autodiscover answers name mailboxes.
fn malformed_answer(message: String, ctx: GraphErrorContext) -> AccountError {
    into_account_error(
        crate::error::GraphError::Json {
            message,
            body: None,
        },
        ctx,
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
    /// An HTTP 200 whose body is malformed, truncated, or not an
    /// `Autodiscover` document is the provider's malformed response
    /// (`Protocol(ParseFailed)`), not an answer with no delegates: it used
    /// to read as an empty list, indistinguishable from a user who has
    /// none. The best-effort policy lives at the call site, not here: `open`
    /// degrades to the config-supplied mailboxes on any `Err` and records
    /// the skipped pass on `skipped_scopes`, so a bad answer is now
    /// reportable instead of silent.
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
        let ctx = GraphErrorContext::graph(AccountOperation::Discover);
        let url = self
            .client
            .outlook_url(AUTODISCOVER_XML_PATH)
            .map_err(|error| into_account_error(error, ctx.clone()))?;
        let xml = self.autodiscover_post(&url, "text/xml", None, body).await?;
        parse_alternative_mailboxes(&xml).map_err(|reason| {
            malformed_answer(format!("Autodiscover alternativeMailboxes: {reason}"), ctx)
        })
    }

    async fn soap_get_user_settings(
        &self,
        email: &str,
        settings: &[&str],
    ) -> Result<Vec<(String, String)>, AccountError> {
        // Autodiscover redirects (`RedirectAddress` to a new email,
        // `RedirectUrl` to a new endpoint) are common for hybrid /
        // on-prem tenants and ride in-body, not as an HTTP 3xx. Follow a
        // bounded chain; the cap guards against a redirect loop.
        const MAX_REDIRECTS: usize = 5;
        let ctx = GraphErrorContext::graph(AccountOperation::Discover);
        let mut url = self
            .client
            .outlook_url(AUTODISCOVER_SOAP_PATH)
            .map_err(|error| into_account_error(error, ctx.clone()))?;
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
            let parsed = parse_user_settings_response(&xml).map_err(|reason| {
                malformed_answer(
                    format!("Autodiscover GetUserSettings: {reason}"),
                    ctx.clone(),
                )
            })?;

            match self.next_step(&parsed, &ctx)? {
                SoapStep::Settings => return Ok(parsed.settings),
                SoapStep::Mailbox(next) => email = next,
                SoapStep::Endpoint(next) => url = next,
            }
        }

        Err(super::graph_error::invalid_account_error(
            AccountOperation::Discover,
            "Autodiscover GetUserSettings exceeded redirect limit",
        ))
    }

    /// Decide what one `GetUserSettings` answer asks for.
    ///
    /// The redirect KIND is read from the protocol's own `ErrorCode`, never
    /// guessed from the shape of the target text: `RedirectAddress` names a
    /// mailbox to query against the same endpoint, `RedirectUrl` names an
    /// endpoint for the same mailbox. The user-level answer wins over the
    /// response-level one, because that is where Exchange puts a per-user
    /// redirect or refusal.
    ///
    /// A `RedirectUrl` endpoint must sit on the configured Outlook origin.
    /// The request carries the account bearer, which bifrost-net attaches
    /// whatever the host, and that token was issued for the configured
    /// resource: following a cross-origin endpoint with it could only hand
    /// it to another host. A hybrid tenant whose Autodiscover points at
    /// another origin needs trust this client does not establish, so that
    /// is `Unsupported(Discover)`, not a provider fault; a redirect answer
    /// that contradicts itself (a code with no target, a target with no
    /// redirect code, a URL named as a mailbox, an endpoint that is not a
    /// URL) is the provider's malformed response.
    fn next_step(
        &self,
        parsed: &UserSettingsResponse,
        ctx: &GraphErrorContext,
    ) -> Result<SoapStep, AccountError> {
        let malformed = |message: String| malformed_answer(message, ctx.clone());
        let failure = |code: &str, message: Option<&str>| {
            super::graph_error::invalid_account_error(
                AccountOperation::Discover,
                format!(
                    "Autodiscover GetUserSettings error {code}: {}",
                    message.unwrap_or("")
                ),
            )
        };

        // A response-level failure other than a redirect ends the lookup.
        if let Some(code) = failing_code(parsed.response.code.as_deref())
            && !is_redirect(code)
        {
            return Err(failure(code, parsed.response.message.as_deref()));
        }
        // A user element that says anything - a code OR a target - is the
        // answer, so a user-level target with no user-level code is judged
        // as the contradiction it is rather than masked by a response-level
        // `NoError`.
        let answer = if parsed.user.code.is_some() || parsed.user.redirect_target.is_some() {
            &parsed.user
        } else {
            &parsed.response
        };
        let code = failing_code(answer.code.as_deref());
        match (code, answer.redirect_target.as_deref()) {
            (None, None) => Ok(SoapStep::Settings),
            (None, Some(_)) => Err(malformed(
                "Autodiscover named a redirect target without a redirect code".to_string(),
            )),
            (Some(code), target) if is_redirect(code) => {
                let Some(target) = target else {
                    return Err(malformed(format!(
                        "Autodiscover {code} carried no redirect target"
                    )));
                };
                if code.eq_ignore_ascii_case(REDIRECT_ADDRESS) {
                    if is_http_url(target) {
                        return Err(malformed(
                            "Autodiscover RedirectAddress named a URL, not a mailbox".to_string(),
                        ));
                    }
                    return Ok(SoapStep::Mailbox(target.to_string()));
                }
                if !is_http_url(target) {
                    return Err(malformed(
                        "Autodiscover RedirectUrl did not name an http or https URL".to_string(),
                    ));
                }
                self.client
                    .admit_outlook_target(target)
                    .map(SoapStep::Endpoint)
                    .map_err(|refusal| {
                        super::graph_error::unsupported_account_error(AccountOperation::Discover)
                            .into_builder()
                            .text(bifrost_types::DiagnosticText::support_only(format!(
                                "Autodiscover redirected to an endpoint this client will not \
                                 send the account credential to: {refusal}"
                            )))
                            .try_build()
                            .expect("valid account error classification")
                    })
            }
            (Some(code), _) => Err(failure(code, answer.message.as_deref())),
        }
    }

    /// Shared transport for both Autodiscover endpoints. Routes the
    /// Bearer through `AccountNet`; classifies failures through the REST
    /// error path with `Protocol::Graph`.
    async fn autodiscover_post(
        &self,
        url: &AdmittedUrl,
        content_type: &str,
        extra_header: Option<(&str, &str)>,
        body: String,
    ) -> Result<String, AccountError> {
        let ctx = GraphErrorContext::graph(AccountOperation::Discover);
        let mut headers = vec![("Content-Type", content_type)];
        if let Some((name, value)) = extra_header {
            headers.push((name, value));
        }
        let resp = self
            .client
            .execute_aux(
                "POST",
                AuxTarget::Bearer(url),
                &headers,
                bytes::Bytes::from(body),
                None,
            )
            .await
            .map_err(|error| into_account_error(error, ctx.clone()))?;

        let status = resp.status;
        if !status.is_success() {
            // The real response headers, not an empty map: `Retry-After` and
            // `WWW-Authenticate` are part of the classification input.
            let response =
                GraphResponseError::from_response(status, resp.headers, cap_status_body(resp.body));
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

// ── Pure parsers ────────────────────────────────────────────

/// The parsed shape of a `GetUserSettings` response. EWS Autodiscover
/// returns failures and redirects INSIDE an HTTP 200 (the in-body
/// `<a:ErrorCode>` / `<a:RedirectTarget>`), so a status-only check reads
/// a failed or redirecting response as an empty settings vec. This
/// captures all three so the caller can act on them.
#[derive(Debug, Default)]
pub(crate) struct UserSettingsResponse {
    pub(crate) settings: Vec<(String, String)>,
    /// The `<a:Response>`-level markers. In a real answer this level is
    /// usually `NoError` even when the user-level answer is a redirect or
    /// a refusal, which is why the two levels are kept apart: reading only
    /// the first `ErrorCode` in document order saw `NoError` and missed
    /// every per-user error and redirect.
    pub(crate) response: SoapAnswer,
    /// The first `<a:UserResponse>`'s markers (one user is requested).
    pub(crate) user: SoapAnswer,
}

/// One level's in-body error and redirect markers.
#[derive(Debug, Default)]
pub(crate) struct SoapAnswer {
    /// `<a:ErrorCode>` (`NoError` / absent on success).
    pub(crate) code: Option<String>,
    pub(crate) message: Option<String>,
    /// `<a:RedirectTarget>`: the SMTP address or URL to retry against;
    /// common for hybrid / on-prem tenants.
    pub(crate) redirect_target: Option<String>,
}

/// Parse `UserSetting` `<Name>`/`<Value>` pairs from a
/// `GetUserSettings` SOAP response. Thin wrapper over
/// `parse_user_settings_response` for the settings-only tests; the
/// production path consumes the full response (error/redirect markers).
#[cfg(test)]
fn parse_user_settings(xml: &str) -> Vec<(String, String)> {
    parse_user_settings_response(xml)
        .expect("well-formed test document")
        .settings
}

/// Tracks that an Autodiscover answer is one complete document with the
/// expected root, so a malformed or truncated HTTP 200 cannot read as a
/// valid answer that happens to be empty.
///
/// quick-xml reports a syntax error as `Err`, but input that simply STOPS
/// (a truncated body) reaches `Eof` with elements still open and no error,
/// and a body that is not XML at all (an HTML error page, an empty body)
/// can produce no element whatever. Both used to end the walk exactly like
/// a complete answer.
struct DocumentShape {
    root: &'static str,
    seen_root: bool,
    depth: usize,
}

impl DocumentShape {
    fn new(root: &'static str) -> Self {
        Self {
            root,
            seen_root: false,
            depth: 0,
        }
    }

    /// An element opened (`self_closing`: and closed at once).
    fn open(&mut self, local_name: &str, self_closing: bool) -> Result<(), String> {
        if self.depth == 0 {
            if self.seen_root {
                return Err("a second root element follows the document".to_string());
            }
            if local_name != self.root {
                return Err(format!(
                    "root element is {local_name}, expected {}",
                    self.root
                ));
            }
            self.seen_root = true;
        }
        if !self_closing {
            self.depth += 1;
        }
        Ok(())
    }

    fn close(&mut self) -> Result<(), String> {
        self.depth = self
            .depth
            .checked_sub(1)
            .ok_or_else(|| "an end tag closes nothing".to_string())?;
        Ok(())
    }

    /// The input ended: fine only after one whole root element.
    fn finish(&self) -> Result<(), String> {
        if !self.seen_root {
            return Err(format!("no {} element in the response", self.root));
        }
        if self.depth != 0 {
            return Err("the response ended inside an open element (truncated)".to_string());
        }
        Ok(())
    }
}

/// Parse a `GetUserSettings` response into settings plus the in-body
/// error / redirect markers. A document that is malformed, truncated, or not
/// a SOAP envelope at all is `Err` with the reason, never an empty answer.
fn parse_user_settings_response(xml: &str) -> Result<UserSettingsResponse, String> {
    let mut reader = Reader::from_str(xml);
    let mut out = UserSettingsResponse::default();
    let mut shape = DocumentShape::new("Envelope");

    let mut in_user_setting = false;
    // Inside the first `<a:UserResponse>`; later ones are ignored, since one
    // user is requested.
    let mut in_user_response = false;
    let mut seen_user_response = false;
    // Inside `<a:UserSettingErrors>`, whose per-setting `ErrorCode`s are
    // not the user's answer.
    let mut in_setting_errors = false;
    let mut current_name = String::new();
    let mut current_value = String::new();
    let mut current_tag = String::new();
    let mut buf = String::new();

    loop {
        match reader.read_event() {
            Ok(Event::Start(ref e)) => {
                let name = e.name().local_name().as_ref().to_owned();
                shape.open(&name, false)?;
                match name.as_str() {
                    "UserSetting" => {
                        in_user_setting = true;
                        current_name.clear();
                        current_value.clear();
                    }
                    "UserResponse" if !seen_user_response => in_user_response = true,
                    "UserSettingErrors" => in_setting_errors = true,
                    _ => {}
                }
                current_tag = name;
                buf.clear();
            }
            Ok(Event::Text(ref e)) => push_text(e, &mut buf)?,
            Ok(Event::GeneralRef(ref e)) => try_push_general_ref(e, &mut buf)?,
            Ok(Event::Empty(ref e)) => {
                shape.open(e.name().local_name().as_ref(), true)?;
            }
            Ok(Event::End(ref e)) => {
                shape.close()?;
                let name = e.name().local_name().as_ref().to_owned();
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
                } else if !in_setting_errors && (in_user_response || !seen_user_response) {
                    // Response-level or user-level markers, never a
                    // per-setting error and never a second user's answer.
                    let answer = if in_user_response {
                        &mut out.user
                    } else {
                        &mut out.response
                    };
                    match current_tag.as_str() {
                        "ErrorCode" if answer.code.is_none() => {
                            answer.code = Some(trimmed.to_string());
                        }
                        "ErrorMessage" if answer.message.is_none() && !trimmed.is_empty() => {
                            answer.message = Some(trimmed.to_string());
                        }
                        "RedirectTarget"
                            if answer.redirect_target.is_none() && !trimmed.is_empty() =>
                        {
                            answer.redirect_target = Some(trimmed.to_string());
                        }
                        _ => {}
                    }
                }
                match name.as_str() {
                    "UserSetting" => {
                        in_user_setting = false;
                        // Keep the pair when a Name is present; an empty
                        // Value is a legitimate setting, not a reason to
                        // drop it.
                        if !current_name.is_empty() {
                            out.settings
                                .push((current_name.clone(), current_value.clone()));
                        }
                    }
                    "UserResponse" if in_user_response => {
                        in_user_response = false;
                        seen_user_response = true;
                    }
                    "UserSettingErrors" => in_setting_errors = false,
                    _ => {}
                }
                buf.clear();
                current_tag.clear();
            }
            Ok(Event::Eof) => break,
            Err(error) => return Err(format!("malformed XML: {error}")),
            _ => {}
        }
    }

    shape.finish()?;
    Ok(out)
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

/// Parse `AlternativeMailbox` elements from an Autodiscover XML response. A
/// document that is malformed, truncated, or not an `Autodiscover` answer
/// at all is `Err` with the reason, never an empty mailbox list.
fn parse_alternative_mailboxes(xml: &str) -> Result<Vec<SharedMailbox>, String> {
    let mut reader = Reader::from_str(xml);
    let mut mailboxes = Vec::new();
    let mut shape = DocumentShape::new("Autodiscover");

    let mut in_alternative_mailbox = false;
    let mut current_type = String::new();
    let mut current_display_name = String::new();
    let mut current_smtp = String::new();
    let mut current_tag = String::new();
    let mut buf = String::new();

    loop {
        match reader.read_event() {
            Ok(Event::Start(ref e)) => {
                let name = e.name().local_name().as_ref().to_owned();
                shape.open(&name, false)?;
                if name == "AlternativeMailbox" {
                    in_alternative_mailbox = true;
                    current_type.clear();
                    current_display_name.clear();
                    current_smtp.clear();
                }
                current_tag = name;
                buf.clear();
            }
            Ok(Event::Text(ref e)) => push_text(e, &mut buf)?,
            Ok(Event::GeneralRef(ref e)) => try_push_general_ref(e, &mut buf)?,
            Ok(Event::Empty(ref e)) => {
                shape.open(e.name().local_name().as_ref(), true)?;
            }
            Ok(Event::End(ref e)) => {
                shape.close()?;
                let name = e.name().local_name().as_ref().to_owned();
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
            Ok(Event::Eof) => break,
            Err(error) => return Err(format!("malformed XML: {error}")),
            _ => {}
        }
    }

    shape.finish()?;
    Ok(mailboxes)
}

/// A text run that does not unescape is a malformed answer, not an empty
/// one: dropping it would splice the text on either side into a different
/// value (an address, say).
fn push_text(e: &quick_xml::events::BytesText<'_>, buf: &mut String) -> Result<(), String> {
    let text = unescape(e.as_ref()).map_err(|error| format!("malformed XML text: {error}"))?;
    buf.push_str(&text);
    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::account::PushMode;
    use crate::client::{GraphClient, ScriptedRestResponse};

    fn test_account(client: GraphClient) -> GraphAccount {
        GraphAccount::new_for_tests(client, PushMode::GraphSubscriptions)
    }

    fn user_settings_xml(name: &str, value: &str) -> String {
        format!(
            r#"<?xml version="1.0" encoding="utf-8"?>
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
                <a:Name>{name}</a:Name>
                <a:Value>{value}</a:Value>
              </a:UserSetting>
            </a:UserSettings>
          </a:UserResponse>
        </a:UserResponses>
      </a:Response>
    </a:GetUserSettingsResponseMessage>
  </s:Body>
</s:Envelope>"#
        )
    }

    /// A per-user answer in the shape Exchange sends it: the response level
    /// reports `NoError` and the user level carries the code and target.
    fn user_answer_xml(code: &str, target: Option<&str>) -> String {
        let target = target
            .map(|target| format!("<a:RedirectTarget>{target}</a:RedirectTarget>"))
            .unwrap_or_default();
        format!(
            r#"<?xml version="1.0" encoding="utf-8"?>
<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/"
            xmlns:a="http://schemas.microsoft.com/exchange/2010/Autodiscover">
  <s:Body>
    <a:GetUserSettingsResponseMessage>
      <a:Response>
        <a:ErrorCode>NoError</a:ErrorCode>
        <a:ErrorMessage />
        <a:UserResponses>
          <a:UserResponse>
            <a:ErrorCode>{code}</a:ErrorCode>
            <a:ErrorMessage>per-user answer</a:ErrorMessage>
            {target}
            <a:UserSettingErrors />
            <a:UserSettings />
          </a:UserResponse>
        </a:UserResponses>
      </a:Response>
    </a:GetUserSettingsResponseMessage>
  </s:Body>
</s:Envelope>"#
        )
    }

    fn redirect_xml(code: &str, target: &str) -> String {
        user_answer_xml(code, Some(target))
    }

    async fn content_mailbox_error(answer: String) -> (bifrost_types::AccountError, usize) {
        let client = GraphClient::new("token");
        client.script_aux([ScriptedRestResponse::text(reqwest::StatusCode::OK, &answer)]);
        let account = test_account(client.clone());
        let error = account
            .discover_content_mailbox("replica@contoso.com")
            .await
            .expect_err("the answer must fail the lookup");
        (error, client.take_aux_requests().len())
    }

    /// The Autodiscover POST itself, which is not a Graph REST call: it
    /// goes to the Autodiscover origin (NOT the Graph host), posts XML, and
    /// carries the bearer. The delegate-discovery variant hits
    /// `/autodiscover/autodiscover.xml` with no `SOAPAction`, and the
    /// request body must name the mailbox and the response schema the
    /// parser expects.
    #[tokio::test]
    async fn delegate_discovery_posts_autodiscover_xml_and_parses_the_mailboxes() {
        let client = GraphClient::new("token");
        client.script_aux([ScriptedRestResponse::text(
            reqwest::StatusCode::OK,
            r#"<?xml version="1.0" encoding="utf-8"?>
<Autodiscover>
  <Response>
    <Account>
      <AlternativeMailbox>
        <Type>Delegate</Type>
        <DisplayName>Sales Team</DisplayName>
        <SmtpAddress>sales@contoso.com</SmtpAddress>
      </AlternativeMailbox>
    </Account>
  </Response>
</Autodiscover>"#,
        )]);
        let account = test_account(client.clone());

        let mailboxes = account
            .discover_shared_mailboxes("user@contoso.com")
            .await
            .expect("discovery succeeds");
        assert_eq!(mailboxes.len(), 1);
        assert_eq!(mailboxes[0].smtp_address, "sales@contoso.com");

        let requests = client.take_aux_requests();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].method, "POST");
        assert_eq!(
            requests[0].url,
            format!(
                "{}/autodiscover/autodiscover.xml",
                crate::client::OUTLOOK_BASE
            )
        );
        assert_eq!(requests[0].header("Content-Type"), Some("text/xml"));
        assert_eq!(requests[0].header("SOAPAction"), None);
        assert!(
            requests[0].bearer,
            "Autodiscover is authenticated with the Graph bearer"
        );
        let body = String::from_utf8(requests[0].body.to_vec()).expect("UTF-8 request body");
        assert!(
            body.contains("<EMailAddress>user@contoso.com</EMailAddress>"),
            "{body}"
        );
        assert!(body.contains("responseschema/2006a"), "{body}");
    }

    /// bifrost-net returns `Err` for every 4xx/5xx before a response
    /// surfaces, so the only status that can reach `autodiscover_post`'s
    /// non-2xx branch is a passed-through 3xx - and when one does, the
    /// response's OWN headers are the classification and telemetry input.
    /// They used to be replaced with an empty map here, which discarded
    /// `Retry-After`, `WWW-Authenticate`, and the request id support reads.
    #[tokio::test]
    async fn a_passed_through_redirect_classifies_with_the_response_headers() {
        let client = GraphClient::new("token");
        client.script_aux([ScriptedRestResponse::text(reqwest::StatusCode::FOUND, "")
            .with_header("Retry-After", "30")
            .with_header("request-id", "req-77")
            .with_header("client-request-id", "crid-77")]);
        let account = test_account(client);

        let error = account
            .discover_shared_mailboxes("user@contoso.com")
            .await
            .expect_err("a redirect is not a discovery result");

        let telemetry = error.telemetry_fields();
        assert_eq!(telemetry.request_id, Some("req-77"));
        assert_eq!(telemetry.trace_id, Some("crid-77"));
    }

    /// A hybrid tenant answers `GetUserSettings` with an in-body
    /// `RedirectAddress` inside an HTTP 200 - bifrost-net never sees a 3xx,
    /// so nothing below this crate follows it. The walk must reissue the
    /// SOAP call for the NEW mailbox against the SAME endpoint, and a
    /// `RedirectUrl` on the configured Outlook origin must move the endpoint
    /// instead. Both redirects ride at the USER level under a response-level
    /// `NoError`, which is how Exchange sends them.
    #[tokio::test]
    async fn a_soap_redirect_chain_reissues_against_the_new_mailbox_then_endpoint() {
        let client = GraphClient::new("token");
        client.script_aux([
            ScriptedRestResponse::text(
                reqwest::StatusCode::OK,
                &redirect_xml("RedirectAddress", "replica@contoso.mail.onmicrosoft.com"),
            ),
            ScriptedRestResponse::text(
                reqwest::StatusCode::OK,
                &redirect_xml(
                    "RedirectUrl",
                    "HTTPS://outlook.office365.com/autodiscover/tenant/autodiscover.svc",
                ),
            ),
            ScriptedRestResponse::text(
                reqwest::StatusCode::OK,
                &user_settings_xml("AutoDiscoverSMTPAddress", "content@contoso.com"),
            ),
        ]);
        let account = test_account(client.clone());

        let mailbox = account
            .discover_content_mailbox("replica@contoso.com")
            .await
            .expect("the redirect chain resolves");
        assert_eq!(mailbox, "content@contoso.com");

        let requests = client.take_aux_requests();
        assert_eq!(requests.len(), 3);
        let soap_url = format!(
            "{}/autodiscover/autodiscover.svc",
            crate::client::OUTLOOK_BASE
        );
        assert_eq!(requests[0].url, soap_url);
        // The address redirect keeps the endpoint and changes the mailbox.
        assert_eq!(requests[1].url, soap_url);
        // The URL redirect moves the endpoint and keeps the mailbox; the
        // admitted URL is the parser's serialization, scheme lower-cased.
        assert_eq!(
            requests[2].url,
            "https://outlook.office365.com/autodiscover/tenant/autodiscover.svc"
        );
        let mailboxes: Vec<String> = requests
            .iter()
            .map(|request| {
                let body = String::from_utf8(request.body.to_vec()).expect("UTF-8 body");
                let start =
                    body.find("<a:Mailbox>").expect("mailbox element") + "<a:Mailbox>".len();
                let end = body.find("</a:Mailbox>").expect("mailbox element end");
                body[start..end].to_string()
            })
            .collect();
        assert_eq!(
            mailboxes,
            vec![
                "replica@contoso.com".to_string(),
                "replica@contoso.mail.onmicrosoft.com".to_string(),
                "replica@contoso.mail.onmicrosoft.com".to_string(),
            ]
        );
        for request in &requests {
            assert_eq!(
                request.header("Content-Type"),
                Some("text/xml; charset=utf-8")
            );
            assert!(
                request
                    .header("SOAPAction")
                    .is_some_and(|action| action.contains("GetUserSettings")),
                "the SOAP endpoint requires the action header"
            );
        }
    }

    /// An in-body `ErrorCode` other than `NoError` is a real failure, not
    /// an empty settings vec: without this the caller reports "response
    /// missing PublicFolderInformation" for what is actually an invalid
    /// user, and the Autodiscover error text is lost.
    #[tokio::test]
    async fn an_in_body_error_code_fails_the_lookup_with_its_text() {
        let client = GraphClient::new("token");
        client.script_aux([ScriptedRestResponse::text(
            reqwest::StatusCode::OK,
            r#"<?xml version="1.0" encoding="utf-8"?>
<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/"
            xmlns:a="http://schemas.microsoft.com/exchange/2010/Autodiscover">
  <s:Body>
    <a:GetUserSettingsResponseMessage>
      <a:Response>
        <a:ErrorCode>InvalidUser</a:ErrorCode>
        <a:ErrorMessage>The user could not be found.</a:ErrorMessage>
      </a:Response>
    </a:GetUserSettingsResponseMessage>
  </s:Body>
</s:Envelope>"#,
        )]);
        let account = test_account(client.clone());

        let error = account
            .discover_public_folder_routing("ghost@contoso.com")
            .await
            .expect_err("an in-body error fails the lookup");
        assert!(matches!(
            error.kind(),
            bifrost_types::AccountErrorKind::Request(bifrost_types::RequestErrorKind::Malformed)
        ));
        assert_eq!(client.take_aux_requests().len(), 1);
    }

    /// The token leak this closes: the Autodiscover POST carries the account
    /// bearer, and bifrost-net attaches it to the first request whatever the
    /// host, so following a `RedirectUrl` off the Outlook origin would hand
    /// the token to whoever the response named. However the endpoint is
    /// spelled, the lookup stops after the one request that returned it.
    #[tokio::test]
    async fn a_cross_origin_redirect_url_is_refused_before_any_request() {
        for target in [
            "https://autodiscover.contoso.com/autodiscover/autodiscover.svc",
            "HTTPS://attacker.example/autodiscover.svc",
            "https://outlook.office365.com@attacker.example/autodiscover.svc",
            "http://outlook.office365.com/autodiscover/autodiscover.svc",
            "https://@outlook.office365.com/autodiscover/autodiscover.svc",
        ] {
            let (error, requests) =
                content_mailbox_error(redirect_xml("RedirectUrl", target)).await;
            assert_eq!(requests, 1, "{target} must not be requested");
            assert!(
                matches!(
                    error.kind(),
                    bifrost_types::AccountErrorKind::Unsupported(AccountOperation::Discover)
                ),
                "{target}: {:?}",
                error.kind()
            );
        }
    }

    /// A redirect answer that contradicts itself is the provider's malformed
    /// response, never guessed into a mailbox or an endpoint.
    #[tokio::test]
    async fn an_inconsistent_redirect_answer_is_a_provider_fault() {
        for answer in [
            // A URL named as a mailbox.
            redirect_xml("RedirectAddress", "https://attacker.example/x"),
            // An endpoint that is not a URL (it must not become a mailbox).
            redirect_xml("RedirectUrl", "https://"),
            redirect_xml("RedirectUrl", "someone@contoso.com"),
            // A redirect code with no target.
            user_answer_xml("RedirectUrl", None),
            // A target with no redirect code.
            redirect_xml("NoError", "someone@contoso.com"),
            // A user-level target with no user-level code at all, under a
            // response-level `NoError` that must not mask it.
            redirect_xml("Omitted", "https://attacker.example/x")
                .replace("<a:ErrorCode>Omitted</a:ErrorCode>", ""),
        ] {
            let (error, requests) = content_mailbox_error(answer).await;
            assert_eq!(requests, 1);
            assert!(
                matches!(error.kind(), bifrost_types::AccountErrorKind::Protocol(_)),
                "{:?}",
                error.kind()
            );
        }
    }

    /// Exchange reports a per-user refusal under a response-level `NoError`.
    /// Reading only the first `ErrorCode` in the document saw `NoError`, so
    /// an unknown user came back as an empty settings list and the caller
    /// reported a missing setting instead of the refusal.
    #[tokio::test]
    async fn a_user_level_error_under_a_response_level_no_error_fails_the_lookup() {
        let (error, requests) = content_mailbox_error(user_answer_xml("InvalidUser", None)).await;
        assert_eq!(requests, 1);
        assert!(matches!(
            error.kind(),
            bifrost_types::AccountErrorKind::Request(bifrost_types::RequestErrorKind::Malformed)
        ));
        assert!(
            format!("{error:?}").contains("InvalidUser"),
            "the refusal names its code: {error:?}"
        );
    }

    /// `text` cut just before the first occurrence of `marker`.
    fn cut_before(text: &str, marker: &str) -> String {
        text[..text.find(marker).expect("marker present")].to_string()
    }

    fn assert_parse_failed(error: &bifrost_types::AccountError) {
        assert_eq!(
            error.kind(),
            &bifrost_types::AccountErrorKind::Protocol(
                bifrost_types::ProtocolErrorKind::ParseFailed
            ),
            "{error:?}"
        );
    }

    /// Bodies an HTTP 200 can carry that are not a `GetUserSettings` answer.
    /// The old parser treated a quick-xml error like end of input and never
    /// checked that the document was complete, so every one of these came
    /// back as an answer with no settings and no error.
    #[test]
    fn a_malformed_user_settings_document_is_an_error_not_an_empty_answer() {
        let whole = user_settings_xml("AutoDiscoverSMTPAddress", "content@contoso.com");
        for (label, body) in [
            ("empty", String::new()),
            ("plain text", "Bad Gateway".to_string()),
            ("wrong root", "<html><body>ok</body></html>".to_string()),
            (
                "truncated inside a setting",
                cut_before(&whole, "</a:UserSetting>"),
            ),
            (
                "truncated after the settings",
                cut_before(&whole, "</a:UserResponse>"),
            ),
            (
                "mismatched end tag",
                whole.replace("</a:Value>", "</a:Name>"),
            ),
            ("two roots", format!("{whole}<s:Envelope/>")),
            (
                "unknown entity in a value",
                whole.replace("content@", "content&bogus;@"),
            ),
        ] {
            assert!(
                parse_user_settings_response(&body).is_err(),
                "{label} must not parse as an answer"
            );
        }
    }

    /// The same for the delegate document: a truncated answer used to yield
    /// the mailboxes before the cut as if they were the whole list, and a
    /// non-Autodiscover body an empty list.
    #[test]
    fn a_malformed_alternative_mailboxes_document_is_an_error_not_a_short_list() {
        let whole = r#"<?xml version="1.0" encoding="utf-8"?>
<Autodiscover>
  <Response>
    <Account>
      <AlternativeMailbox><SmtpAddress>sales@contoso.com</SmtpAddress></AlternativeMailbox>
      <AlternativeMailbox><SmtpAddress>eng@contoso.com</SmtpAddress></AlternativeMailbox>
    </Account>
  </Response>
</Autodiscover>"#;
        assert_eq!(
            parse_alternative_mailboxes(whole)
                .expect("the whole document parses")
                .len(),
            2
        );
        for (label, body) in [
            ("empty", String::new()),
            ("wrong root", "<html><body>ok</body></html>".to_string()),
            (
                "truncated after one mailbox",
                cut_before(whole, "<AlternativeMailbox><SmtpAddress>eng"),
            ),
            (
                "truncated before the root closes",
                cut_before(whole, "</Autodiscover>"),
            ),
            (
                "mismatched end tag",
                whole.replace("</Account>", "</Response>"),
            ),
            // Dropping the reference would read `sales@contoso.com`, a
            // different mailbox from the one the server named.
            ("unknown entity", whole.replace("sales@", "sales&bogus;@")),
            (
                "invalid character reference",
                whole.replace("sales@", "sales&#xD800;@"),
            ),
        ] {
            assert!(
                parse_alternative_mailboxes(&body).is_err(),
                "{label} must not parse as a mailbox list"
            );
        }
    }

    /// End to end on both `GetUserSettings` lookups: a truncated HTTP 200 is
    /// the provider's malformed response. The old code read the truncated
    /// answer as one with no settings and reported the missing setting as
    /// `Request(Malformed)`, blaming the caller for the provider's body.
    #[tokio::test]
    async fn a_truncated_user_settings_answer_is_the_providers_parse_failure() {
        let truncated = cut_before(
            &user_settings_xml("AutoDiscoverSMTPAddress", "content@contoso.com"),
            "</a:UserSetting>",
        );
        let (error, requests) = content_mailbox_error(truncated).await;
        assert_eq!(requests, 1);
        assert_parse_failed(&error);

        let client = GraphClient::new("token");
        client.script_aux([ScriptedRestResponse::text(
            reqwest::StatusCode::OK,
            &cut_before(
                &user_settings_xml("PublicFolderInformation", "pf@contoso.com"),
                "</a:UserSetting>",
            ),
        )]);
        let error = test_account(client.clone())
            .discover_public_folder_routing("user@contoso.com")
            .await
            .expect_err("a truncated answer fails the lookup");
        assert_parse_failed(&error);
        assert_eq!(client.take_aux_requests().len(), 1);
    }

    /// Delegate discovery on a malformed HTTP 200 fails instead of reporting
    /// "no delegates", so `open`'s best-effort degradation records a skipped
    /// pass rather than silently opening without them. The old code returned
    /// `Ok` with an empty list.
    #[tokio::test]
    async fn a_malformed_delegate_answer_fails_discovery_instead_of_finding_none() {
        let client = GraphClient::new("token");
        client.script_aux([ScriptedRestResponse::text(
            reqwest::StatusCode::OK,
            "<Autodiscover><Response><Account><AlternativeMailbox>",
        )]);
        let error = test_account(client)
            .discover_shared_mailboxes("user@contoso.com")
            .await
            .expect_err("a truncated answer is not an empty one");
        assert_parse_failed(&error);
    }

    /// A per-setting `ErrorCode` inside `UserSettingErrors` is not the
    /// user's answer: the requested setting that did resolve is returned.
    #[test]
    fn a_per_setting_error_is_not_the_user_answer() {
        let xml = r#"<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/"
            xmlns:a="http://schemas.microsoft.com/exchange/2010/Autodiscover">
  <s:Body><a:GetUserSettingsResponseMessage><a:Response>
    <a:ErrorCode>NoError</a:ErrorCode>
    <a:UserResponses><a:UserResponse>
      <a:ErrorCode>NoError</a:ErrorCode>
      <a:UserSettingErrors><a:UserSettingError>
        <a:ErrorCode>SettingIsNotAvailable</a:ErrorCode>
        <a:SettingName>InternalRpcClientServer</a:SettingName>
      </a:UserSettingError></a:UserSettingErrors>
      <a:UserSettings><a:UserSetting>
        <a:Name>PublicFolderInformation</a:Name><a:Value>pf@contoso.com</a:Value>
      </a:UserSetting></a:UserSettings>
    </a:UserResponse></a:UserResponses>
  </a:Response></a:GetUserSettingsResponseMessage></s:Body>
</s:Envelope>"#;
        let parsed = parse_user_settings_response(xml).expect("well-formed");
        assert_eq!(parsed.user.code.as_deref(), Some("NoError"));
        assert_eq!(parsed.response.code.as_deref(), Some("NoError"));
        assert_eq!(
            parsed.settings,
            vec![(
                "PublicFolderInformation".to_string(),
                "pf@contoso.com".to_string()
            )]
        );
    }

    /// An unusable Graph api-base must not leave Autodiscover pointing at the
    /// production Outlook origin: the derived origin is unusable too, and
    /// nothing is sent.
    #[tokio::test]
    async fn an_unusable_api_base_sends_no_autodiscover_request() {
        let client = GraphClient::with_api_base("not a url", "token");
        client.script_aux([]);
        let account = test_account(client.clone());
        let error = account
            .discover_shared_mailboxes("user@contoso.com")
            .await
            .expect_err("an unusable base refuses");
        assert!(matches!(
            error.kind(),
            bifrost_types::AccountErrorKind::Request(bifrost_types::RequestErrorKind::Malformed)
        ));
        assert_eq!(client.wire_attempts(), 0);
    }

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
        let result = parse_alternative_mailboxes(xml).expect("well-formed");
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
        let result = parse_alternative_mailboxes(xml).expect("well-formed");
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
        assert!(
            parse_alternative_mailboxes(xml)
                .expect("well-formed")
                .is_empty()
        );
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
        assert!(
            parse_alternative_mailboxes(xml)
                .expect("well-formed")
                .is_empty()
        );
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
        let parsed = parse_user_settings_response(xml).expect("well-formed");
        assert!(parsed.settings.is_empty());
        assert_eq!(parsed.response.code.as_deref(), Some("InvalidUser"));
        assert_eq!(
            parsed.response.message.as_deref(),
            Some("The user could not be found.")
        );
        // The user element carried no code of its own.
        assert_eq!(parsed.user.code, None);
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
        let parsed = parse_user_settings_response(xml).expect("well-formed");
        assert_eq!(
            parsed.response.redirect_target.as_deref(),
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
        let parsed = parse_user_settings_response(xml).expect("well-formed");
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
