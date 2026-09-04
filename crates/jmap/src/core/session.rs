#[cfg(feature = "mail")]
use crate::email::{MailCapabilities, SubmissionCapabilities};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Session {
    #[serde(rename = "capabilities")]
    #[serde(deserialize_with = "deserialize_capabilities_map")]
    capabilities: HashMap<String, Capabilities>,

    #[serde(rename = "accounts")]
    accounts: HashMap<String, Account>,

    #[serde(rename = "primaryAccounts")]
    primary_accounts: HashMap<String, String>,

    #[serde(rename = "username")]
    username: String,

    #[serde(rename = "apiUrl")]
    api_url: String,

    #[serde(rename = "downloadUrl")]
    download_url: String,

    #[serde(rename = "uploadUrl")]
    upload_url: String,

    #[serde(rename = "eventSourceUrl")]
    event_source_url: String,

    #[serde(rename = "state")]
    state: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Account {
    #[serde(rename = "name")]
    name: String,

    #[serde(rename = "isPersonal")]
    is_personal: bool,

    #[serde(rename = "isReadOnly")]
    is_read_only: bool,

    #[serde(rename = "accountCapabilities")]
    #[serde(deserialize_with = "deserialize_capabilities_map")]
    account_capabilities: HashMap<String, Capabilities>,
}

/// Session/account capability value. The correct variant is selected by
/// the map key (capability URI), not by the value shape.
#[derive(Debug, Clone, Serialize)]
#[serde(untagged)]
#[non_exhaustive]
pub(crate) enum Capabilities {
    Core(CoreCapabilities),
    #[cfg(feature = "mail")]
    Mail(MailCapabilities),
    #[cfg(feature = "mail")]
    Submission(SubmissionCapabilities),
    /// The `urn:ietf:params:jmap:core` block was present but did not
    /// parse as [`CoreCapabilities`] (a limit sent as a string, say).
    /// Kept as its own variant rather than folded into `Other` so that a
    /// present-but-malformed core block reads as the INVALID lane instead
    /// of the unadvertised one: the two map to different recovery classes
    /// (`Protocol(ContractViolation)` versus `SyncState(CapabilityChanged)`),
    /// and `Other` is indistinguishable from absent at every reader.
    CoreMalformed(serde_json::Value),
    WebSocket(WebSocketCapabilities),
    #[cfg(feature = "mail")]
    Sieve(SieveCapabilities),
    #[cfg(feature = "quota")]
    Quota(QuotaCapabilities),
    #[cfg(feature = "blob")]
    Blob(BlobCapabilities),
    #[cfg(feature = "calendars")]
    Calendars(CalendarsCapabilities),
    #[cfg(feature = "contacts")]
    Contacts(ContactsCapabilities),
    Principals(PrincipalsCapabilities),
    PrincipalsOwner(PrincipalsOwnerCapabilities),
    Other(serde_json::Value),
}

/// Custom deserializer for `HashMap<String, Capabilities>` that
/// dispatches to the correct `Capabilities` variant based on the URI
/// key, rather than relying on `#[serde(untagged)]` trial-and-error.
fn deserialize_capabilities_map<'de, D>(
    deserializer: D,
) -> Result<HashMap<String, Capabilities>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let raw: HashMap<String, JsonValue> = HashMap::deserialize(deserializer)?;
    let mut result = HashMap::with_capacity(raw.len());

    /// Deserialize a capability value as a typed struct, falling back to
    /// `Other(original_value)` on parse failure. Serializes to a string
    /// first to avoid cloning the Value - `from_value` consumes on error.
    macro_rules! try_cap {
        ($value:expr, $variant:ident) => {{
            let s = serde_json::to_string(&$value).unwrap();
            match serde_json::from_str(&s) {
                Ok(v) => Capabilities::$variant(v),
                Err(_) => Capabilities::Other($value),
            }
        }};
    }

    for (key, value) in raw {
        let cap = match key.as_str() {
            // The core block is the one capability whose malformed shape
            // must not degrade to "absent" - see `Capabilities::CoreMalformed`.
            "urn:ietf:params:jmap:core" => {
                // Cloned rather than string-round-tripped: `from_value`
                // consumes the value on error, and the malformed value has
                // to survive into `CoreMalformed`. The core object is a
                // handful of scalars, parsed once per session.
                match serde_json::from_value(value.clone()) {
                    Ok(core) => Capabilities::Core(core),
                    Err(_) => Capabilities::CoreMalformed(value),
                }
            }
            #[cfg(feature = "mail")]
            "urn:ietf:params:jmap:mail" => try_cap!(value, Mail),
            #[cfg(feature = "mail")]
            "urn:ietf:params:jmap:submission" => try_cap!(value, Submission),
            "urn:ietf:params:jmap:websocket" => try_cap!(value, WebSocket),
            #[cfg(feature = "mail")]
            "urn:ietf:params:jmap:sieve" => try_cap!(value, Sieve),
            #[cfg(feature = "quota")]
            "urn:ietf:params:jmap:quota" => try_cap!(value, Quota),
            #[cfg(feature = "blob")]
            "urn:ietf:params:jmap:blob" => try_cap!(value, Blob),
            #[cfg(feature = "calendars")]
            "urn:ietf:params:jmap:calendars" => try_cap!(value, Calendars),
            #[cfg(feature = "contacts")]
            "urn:ietf:params:jmap:contacts" => try_cap!(value, Contacts),
            "urn:ietf:params:jmap:principals" => try_cap!(value, Principals),
            "urn:ietf:params:jmap:principals:owner" => try_cap!(value, PrincipalsOwner),
            _ => Capabilities::Other(value),
        };
        result.insert(key, cap);
    }

    Ok(result)
}

/// `urn:ietf:params:jmap:core` limits.
///
/// Every limit is `Option<usize>`, not a zero-filled `usize`. RFC 8620 §2
/// makes all of them mandatory members of the core object, so an omitted
/// one and an advertised `0` are two different server bugs with two
/// different honest readings - "the server told us nothing" versus "the
/// server told us a limit that forbids every request" - and a blanket
/// `#[serde(default)]` merged them into the same zero. The readers that
/// enforce a bound (`CallLimit`, the WebSocket frame-size guard) and the
/// reader that validates the session (`sync::capabilities::build`) need the
/// distinction to land in the right lane.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub(crate) struct CoreCapabilities {
    #[serde(rename = "maxSizeUpload")]
    max_size_upload: Option<usize>,

    #[serde(rename = "maxConcurrentUpload")]
    max_concurrent_upload: Option<usize>,

    #[serde(rename = "maxSizeRequest")]
    max_size_request: Option<usize>,

    #[serde(rename = "maxConcurrentRequests")]
    max_concurrent_requests: Option<usize>,

    #[serde(rename = "maxCallsInRequest")]
    max_calls_in_request: Option<usize>,

    #[serde(rename = "maxObjectsInGet")]
    max_objects_in_get: Option<usize>,

    #[serde(rename = "maxObjectsInSet")]
    max_objects_in_set: Option<usize>,

    #[serde(rename = "collationAlgorithms")]
    collation_algorithms: Vec<String>,
}

/// The three states the core capability block can be in, which the
/// two-state `Option<&CoreCapabilities>` could not express.
#[derive(Debug, Clone, Copy)]
pub(crate) enum CoreCapabilityState<'a> {
    /// No `urn:ietf:params:jmap:core` key in the session at all.
    Absent,
    /// Present, but not parseable as the RFC 8620 core object.
    Malformed,
    Present(&'a CoreCapabilities),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct WebSocketCapabilities {
    #[serde(rename = "url")]
    url: String,
    #[serde(rename = "supportsPush")]
    #[serde(default)]
    supports_push: bool,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub(crate) struct SieveCapabilities {
    #[serde(rename = "implementation")]
    implementation: Option<String>,
    #[serde(rename = "maxSizeScriptName")]
    max_script_name: Option<usize>,
    #[serde(rename = "maxSizeScript")]
    max_script_size: Option<usize>,
    #[serde(rename = "maxNumberScripts")]
    max_scripts: Option<usize>,
    #[serde(rename = "maxNumberRedirects")]
    max_redirects: Option<usize>,
    #[serde(rename = "sieveExtensions")]
    extensions: Vec<String>,
    #[serde(rename = "notificationMethods")]
    notification_methods: Option<Vec<String>>,
    #[serde(rename = "externalLists")]
    ext_lists: Option<Vec<String>>,
}

/// Capabilities for `urn:ietf:params:jmap:quota` (RFC 9425).
///
/// Empty capability object per spec - presence indicates quota support.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct QuotaCapabilities {}

/// Capabilities for `urn:ietf:params:jmap:blob` (RFC 9404).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct BlobCapabilities {
    #[serde(rename = "maxSizeBlobSet")]
    #[serde(default)]
    max_size_blob_set: Option<u64>,

    #[serde(rename = "supportedDigestAlgorithms")]
    #[serde(default)]
    supported_digest_algorithms: Vec<String>,

    #[serde(rename = "supportedTypeNames")]
    #[serde(default)]
    supported_type_names: Vec<String>,
}

/// Capabilities for `urn:ietf:params:jmap:calendars`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct CalendarsCapabilities {
    #[serde(rename = "mayCreateCalendar")]
    #[serde(default)]
    may_create_calendar: bool,

    #[serde(rename = "maxCalendarsPerEvent")]
    #[serde(default)]
    max_calendars_per_event: Option<usize>,
}

/// Capabilities for `urn:ietf:params:jmap:contacts`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ContactsCapabilities {
    #[serde(rename = "mayCreateAddressBook")]
    #[serde(default)]
    may_create_address_book: bool,

    #[serde(rename = "maxAddressBooksPerCard")]
    #[serde(default)]
    max_address_books_per_card: Option<usize>,
}

/// Capabilities for `urn:ietf:params:jmap:principals`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct PrincipalsCapabilities {
    #[serde(rename = "currentUserPrincipalId")]
    #[serde(default)]
    current_user_principal_id: Option<crate::principal::PrincipalId>,

    #[serde(rename = "accountIdForPrincipal")]
    #[serde(default)]
    account_id_for_principal: Option<crate::core::id::AccountId>,
}

macro_rules! session_cap_accessor {
    ($(#[$meta:meta])* $method:ident, $cap_marker:ty, $variant:ident, $return_type:ty) => {
        $(#[$meta])*
        pub(crate) fn $method(&self) -> Option<&$return_type> {
            self.capabilities
                .get(<$cap_marker as crate::core::capability::Capability>::URI)
                .and_then(|v| match v {
                    Capabilities::$variant(c) => Some(c),
                    _ => None,
                })
        }
    };
}

impl Session {
    pub(crate) fn capabilities(&self) -> impl Iterator<Item = &String> {
        self.capabilities.keys()
    }

    pub(crate) fn capability(&self, capability: impl AsRef<str>) -> Option<&Capabilities> {
        self.capabilities.get(capability.as_ref())
    }

    pub(crate) fn has_capability(&self, capability: impl AsRef<str>) -> bool {
        self.capabilities.contains_key(capability.as_ref())
    }

    /// Get a typed capability configuration by its capability marker type.
    ///
    /// Returns `None` if the server does not advertise the capability.
    ///
    /// ```ignore
    /// use crate::core::capability::{Capability, Mail};
    /// if let Some(mail) = session.capability_config::<Mail>() {
    ///     println!("max mailbox depth: {}", mail.max_mailbox_depth());
    /// }
    /// ```
    pub(crate) fn capability_config<C: super::capability::Capability>(&self) -> Option<C::Config> {
        let cap = self.capabilities.get(C::URI)?;
        // Serialize the enum variant to Value, then deserialize into Config.
        // This is a one-time cost per access (session parsing is infrequent).
        let value = serde_json::to_value(cap).ok()?;
        serde_json::from_value(value).ok()
    }

    session_cap_accessor!(
        websocket_capabilities,
        crate::core::capability::WebSocket,
        WebSocket,
        WebSocketCapabilities
    );
    session_cap_accessor!(
        core_capabilities,
        crate::core::capability::Core,
        Core,
        CoreCapabilities
    );

    /// The core capability block as a three-state: absent, present but
    /// unparseable, or present and typed. `core_capabilities()` collapses
    /// the first two into `None`; every reader that has to CLASSIFY a bad
    /// session (rather than merely decline to enforce a bound) must use
    /// this instead.
    pub(crate) fn core_capability_state(&self) -> CoreCapabilityState<'_> {
        match self
            .capabilities
            .get(<crate::core::capability::Core as crate::core::capability::Capability>::URI)
        {
            None => CoreCapabilityState::Absent,
            Some(Capabilities::Core(core)) => CoreCapabilityState::Present(core),
            Some(_) => CoreCapabilityState::Malformed,
        }
    }
    session_cap_accessor!(
        #[cfg(feature = "mail")]
        mail_capabilities,
        crate::core::capability::Mail,
        Mail,
        MailCapabilities
    );
    session_cap_accessor!(
        #[cfg(feature = "mail")]
        submission_capabilities,
        crate::core::capability::Submission,
        Submission,
        SubmissionCapabilities
    );
    session_cap_accessor!(
        #[cfg(feature = "mail")]
        sieve_capabilities,
        crate::core::capability::Sieve,
        Sieve,
        SieveCapabilities
    );
    session_cap_accessor!(
        #[cfg(feature = "quota")]
        quota_capabilities,
        crate::core::capability::Quota,
        Quota,
        QuotaCapabilities
    );
    session_cap_accessor!(
        #[cfg(feature = "blob")]
        blob_capabilities,
        crate::core::capability::Blob,
        Blob,
        BlobCapabilities
    );
    session_cap_accessor!(
        #[cfg(feature = "calendars")]
        calendars_capabilities,
        crate::core::capability::Calendars,
        Calendars,
        CalendarsCapabilities
    );
    session_cap_accessor!(
        #[cfg(feature = "contacts")]
        contacts_capabilities,
        crate::core::capability::Contacts,
        Contacts,
        ContactsCapabilities
    );
    session_cap_accessor!(
        principals_capabilities,
        crate::core::capability::Principals,
        Principals,
        PrincipalsCapabilities
    );
    session_cap_accessor!(
        principals_owner_capabilities,
        crate::core::capability::PrincipalsOwner,
        PrincipalsOwner,
        PrincipalsOwnerCapabilities
    );

    pub(crate) fn accounts(&self) -> impl Iterator<Item = &String> {
        self.accounts.keys()
    }

    pub(crate) fn account(&self, account: &str) -> Option<&Account> {
        self.accounts.get(account)
    }

    pub(crate) fn primary_accounts(&self) -> impl Iterator<Item = (&String, &String)> {
        self.primary_accounts.iter()
    }

    /// Stable fallback account selection for generic request construction.
    /// Capability-specific callers should select their own primary account.
    pub(crate) fn default_account_id(&self) -> Option<&str> {
        self.primary_accounts()
            .min_by_key(|(capability, _)| *capability)
            .map(|(_, account_id)| account_id.as_str())
    }

    pub(crate) fn username(&self) -> &str {
        &self.username
    }

    pub(crate) fn api_url(&self) -> &str {
        &self.api_url
    }

    pub(crate) fn download_url(&self) -> &str {
        &self.download_url
    }

    pub(crate) fn upload_url(&self) -> &str {
        &self.upload_url
    }

    pub(crate) fn event_source_url(&self) -> &str {
        &self.event_source_url
    }

    pub(crate) fn state(&self) -> &str {
        &self.state
    }
}

impl Account {
    pub(crate) fn name(&self) -> &str {
        &self.name
    }

    pub(crate) fn is_personal(&self) -> bool {
        self.is_personal
    }

    pub(crate) fn is_read_only(&self) -> bool {
        self.is_read_only
    }

    pub(crate) fn capabilities(&self) -> impl Iterator<Item = &String> {
        self.account_capabilities.keys()
    }

    pub(crate) fn capability(&self, capability: &str) -> Option<&Capabilities> {
        self.account_capabilities.get(capability)
    }
}

impl CoreCapabilities {
    pub(crate) fn max_size_upload(&self) -> Option<usize> {
        self.max_size_upload
    }

    pub(crate) fn max_concurrent_upload(&self) -> Option<usize> {
        self.max_concurrent_upload
    }

    pub(crate) fn max_size_request(&self) -> Option<usize> {
        self.max_size_request
    }

    pub(crate) fn max_concurrent_requests(&self) -> Option<usize> {
        self.max_concurrent_requests
    }

    pub(crate) fn max_calls_in_request(&self) -> Option<usize> {
        self.max_calls_in_request
    }

    pub(crate) fn max_objects_in_get(&self) -> Option<usize> {
        self.max_objects_in_get
    }

    pub(crate) fn max_objects_in_set(&self) -> Option<usize> {
        self.max_objects_in_set
    }

    pub(crate) fn collation_algorithms(&self) -> &[String] {
        &self.collation_algorithms
    }
}

impl WebSocketCapabilities {
    pub(crate) fn url(&self) -> &str {
        &self.url
    }

    pub(crate) fn supports_push(&self) -> bool {
        self.supports_push
    }
}

impl SieveCapabilities {
    pub(crate) fn max_script_name_size(&self) -> usize {
        self.max_script_name.unwrap_or(512)
    }

    pub(crate) fn max_script_size(&self) -> Option<usize> {
        self.max_script_size
    }

    pub(crate) fn max_number_scripts(&self) -> Option<usize> {
        self.max_scripts
    }

    pub(crate) fn max_number_redirects(&self) -> Option<usize> {
        self.max_redirects
    }

    pub(crate) fn sieve_extensions(&self) -> &[String] {
        &self.extensions
    }

    pub(crate) fn notification_methods(&self) -> Option<&[String]> {
        self.notification_methods.as_deref()
    }

    pub(crate) fn external_lists(&self) -> Option<&[String]> {
        self.ext_lists.as_deref()
    }
}

impl BlobCapabilities {
    pub(crate) fn max_size_blob_set(&self) -> Option<u64> {
        self.max_size_blob_set
    }

    pub(crate) fn supported_digest_algorithms(&self) -> &[String] {
        &self.supported_digest_algorithms
    }

    pub(crate) fn supported_type_names(&self) -> &[String] {
        &self.supported_type_names
    }
}

impl CalendarsCapabilities {
    pub(crate) fn may_create_calendar(&self) -> bool {
        self.may_create_calendar
    }

    pub(crate) fn max_calendars_per_event(&self) -> Option<usize> {
        self.max_calendars_per_event
    }
}

impl ContactsCapabilities {
    pub(crate) fn may_create_address_book(&self) -> bool {
        self.may_create_address_book
    }

    pub(crate) fn max_address_books_per_card(&self) -> Option<usize> {
        self.max_address_books_per_card
    }
}

/// Capabilities for `urn:ietf:params:jmap:principals:owner` (RFC 9670).
///
/// Account-level capability indicating the owner of the account's data.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct PrincipalsOwnerCapabilities {
    #[serde(rename = "accountIdForPrincipal")]
    #[serde(default)]
    account_id_for_principal: Option<crate::core::id::AccountId>,

    #[serde(rename = "principalId")]
    #[serde(default)]
    principal_id: Option<crate::principal::PrincipalId>,
}

impl PrincipalsOwnerCapabilities {
    pub(crate) fn account_id_for_principal(&self) -> Option<&crate::core::id::AccountId> {
        self.account_id_for_principal.as_ref()
    }

    pub(crate) fn principal_id(&self) -> Option<&crate::principal::PrincipalId> {
        self.principal_id.as_ref()
    }
}

impl PrincipalsCapabilities {
    pub(crate) fn current_user_principal_id(&self) -> Option<&crate::principal::PrincipalId> {
        self.current_user_principal_id.as_ref()
    }

    pub(crate) fn account_id_for_principal(&self) -> Option<&crate::core::id::AccountId> {
        self.account_id_for_principal.as_ref()
    }
}

pub(crate) trait URLParser: Sized {
    fn parse(value: &str) -> Option<Self>;
}

/// The set to percent-encode when substituting a value into a URI
/// template variable.
///
/// RFC 6570 §3.2.2 (simple string expansion, the only form JMAP session
/// templates use) says a value is expanded by percent-encoding every
/// character that is not *unreserved* - ALPHA / DIGIT / `-` / `.` / `_`
/// / `~`. An ad-hoc deny list is the wrong shape here: it silently lets
/// through whatever reserved character nobody thought of, and each one
/// is a different injection. `+` in a query-position `{type}` is the
/// cheapest example - `application/ld+json` arrives at the server as
/// `application/ld json` - but `;`, `@`, `!`, `,` and `$` all carry
/// delimiter meaning somewhere in RFC 3986.
pub(crate) const TEMPLATE_VALUE: &percent_encoding::AsciiSet = &percent_encoding::NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'.')
    .remove(b'_')
    .remove(b'~');

/// Percent-encode `value` for substitution into a URI template variable.
pub(crate) fn encode_template_value(value: &str) -> percent_encoding::PercentEncode<'_> {
    percent_encoding::utf8_percent_encode(value, TEMPLATE_VALUE)
}

#[non_exhaustive]
pub(crate) enum URLPart<T: URLParser> {
    Value(String),
    Parameter(T),
}

impl<T: URLParser> URLPart<T> {
    pub(crate) fn parse(url: &str) -> crate::Result<Vec<URLPart<T>>> {
        let mut parts = Vec::new();
        let mut buf = String::with_capacity(url.len());
        let mut in_parameter = false;

        for ch in url.chars() {
            match ch {
                '{' => {
                    if in_parameter {
                        return Err(crate::Error::InvalidUrl(url.to_string()));
                    }
                    if !buf.is_empty() {
                        parts.push(URLPart::Value(std::mem::take(&mut buf)));
                    }
                    in_parameter = true;
                }
                '}' => {
                    if in_parameter && !buf.is_empty() {
                        parts.push(URLPart::Parameter(T::parse(&buf).ok_or_else(|| {
                            crate::Error::InvalidUrl(format!(
                                "Invalid parameter '{buf}' in URL: {url}"
                            ))
                        })?));
                        buf.clear();
                    } else {
                        return Err(crate::Error::InvalidUrl(url.to_string()));
                    }
                    in_parameter = false;
                }
                _ => {
                    buf.push(ch);
                }
            }
        }

        if in_parameter {
            return Err(crate::Error::InvalidUrl(url.to_string()));
        }

        if !buf.is_empty() {
            parts.push(URLPart::Value(std::mem::take(&mut buf)));
        }

        Ok(parts)
    }
}

#[cfg(test)]
mod tests {
    use super::Session;

    fn session_with_multiple_primaries() -> Session {
        serde_json::from_value(serde_json::json!({
            "capabilities": {},
            "accounts": {},
            "primaryAccounts": {
                "urn:ietf:params:jmap:mail": "mail-account",
                "urn:ietf:params:jmap:calendars": "calendar-account",
                "urn:ietf:params:jmap:contacts": "contacts-account"
            },
            "username": "user@example.test",
            "apiUrl": "https://example.test/jmap",
            "downloadUrl": "https://example.test/download/{accountId}/{blobId}",
            "uploadUrl": "https://example.test/upload/{accountId}",
            "eventSourceUrl": "https://example.test/events",
            "state": "state-1"
        }))
        .expect("session fixture decodes")
    }

    #[test]
    fn default_account_id_is_stable_across_session_map_seeds() {
        for _ in 0..32 {
            let session = session_with_multiple_primaries();
            assert_eq!(session.default_account_id(), Some("calendar-account"));
        }
    }
}
