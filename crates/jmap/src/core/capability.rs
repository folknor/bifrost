use serde::de::DeserializeOwned;

/// A JMAP capability identified by its URN.
///
/// Capabilities with session-level configuration should set `Config`
/// to their configuration struct. Capabilities with no configuration
/// (empty JSON object) should use `()`.
pub trait Capability {
    const URI: &'static str;

    /// Session-level capability configuration type.
    type Config: DeserializeOwned + Send + Sync + 'static;
}

use super::session;

pub struct Core;
impl Capability for Core {
    const URI: &'static str = "urn:ietf:params:jmap:core";
    type Config = session::CoreCapabilities;
}

pub struct Mail;
impl Capability for Mail {
    const URI: &'static str = "urn:ietf:params:jmap:mail";
    #[cfg(feature = "mail")]
    type Config = crate::email::MailCapabilities;
    #[cfg(not(feature = "mail"))]
    type Config = serde_json::Value;
}

pub struct Submission;
impl Capability for Submission {
    const URI: &'static str = "urn:ietf:params:jmap:submission";
    #[cfg(feature = "mail")]
    type Config = crate::email::SubmissionCapabilities;
    #[cfg(not(feature = "mail"))]
    type Config = serde_json::Value;
}

pub struct VacationResponseCap;
impl Capability for VacationResponseCap {
    const URI: &'static str = "urn:ietf:params:jmap:vacationresponse";
    type Config = serde_json::Value;
}

pub struct Contacts;
impl Capability for Contacts {
    const URI: &'static str = "urn:ietf:params:jmap:contacts";
    #[cfg(feature = "contacts")]
    type Config = session::ContactsCapabilities;
    #[cfg(not(feature = "contacts"))]
    type Config = serde_json::Value;
}

pub struct ContactsParse;
impl Capability for ContactsParse {
    const URI: &'static str = "urn:ietf:params:jmap:contacts:parse";
    type Config = serde_json::Value;
}

pub struct Calendars;
impl Capability for Calendars {
    const URI: &'static str = "urn:ietf:params:jmap:calendars";
    #[cfg(feature = "calendars")]
    type Config = session::CalendarsCapabilities;
    #[cfg(not(feature = "calendars"))]
    type Config = serde_json::Value;
}

pub struct CalendarsParse;
impl Capability for CalendarsParse {
    const URI: &'static str = "urn:ietf:params:jmap:calendars:parse";
    type Config = serde_json::Value;
}

pub struct Blob;
impl Capability for Blob {
    const URI: &'static str = "urn:ietf:params:jmap:blob";
    #[cfg(feature = "blob")]
    type Config = session::BlobCapabilities;
    #[cfg(not(feature = "blob"))]
    type Config = serde_json::Value;
}

pub struct Quota;
impl Capability for Quota {
    const URI: &'static str = "urn:ietf:params:jmap:quota";
    #[cfg(feature = "quota")]
    type Config = session::QuotaCapabilities;
    #[cfg(not(feature = "quota"))]
    type Config = serde_json::Value;
}

pub struct WebSocket;
impl Capability for WebSocket {
    const URI: &'static str = "urn:ietf:params:jmap:websocket";
    type Config = session::WebSocketCapabilities;
}

pub struct Sieve;
impl Capability for Sieve {
    const URI: &'static str = "urn:ietf:params:jmap:sieve";
    #[cfg(feature = "mail")]
    type Config = session::SieveCapabilities;
    #[cfg(not(feature = "mail"))]
    type Config = serde_json::Value;
}

pub struct Principals;
impl Capability for Principals {
    const URI: &'static str = "urn:ietf:params:jmap:principals";
    type Config = session::PrincipalsCapabilities;
}

pub struct PrincipalsOwner;
impl Capability for PrincipalsOwner {
    const URI: &'static str = "urn:ietf:params:jmap:principals:owner";
    type Config = session::PrincipalsOwnerCapabilities;
}
