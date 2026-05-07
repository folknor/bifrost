#![forbid(unsafe_code)]
#![doc = include_str!("../README.md")]

#[cfg(all(feature = "tls-rustls", feature = "tls-native"))]
compile_error!("Features `tls-rustls` and `tls-native` are mutually exclusive. Choose one.");

#[cfg(not(any(feature = "tls-rustls", feature = "tls-native")))]
compile_error!("Either `tls-rustls` or `tls-native` must be enabled.");

pub mod account;
#[cfg(feature = "contacts")]
pub mod address_book;
pub mod blob;
#[cfg(feature = "calendars")]
pub mod calendar;
#[cfg(feature = "calendars")]
pub mod calendar_event;
#[cfg(feature = "calendars")]
pub mod calendar_event_notification;
pub mod client;
#[cfg(feature = "contacts")]
pub mod contact_card;
pub mod core;
#[cfg(feature = "mail")]
pub mod email;
#[cfg(feature = "mail")]
pub mod email_submission;
pub mod event_source;
#[cfg(feature = "mail")]
pub mod identity;
#[cfg(feature = "mail")]
pub mod mailbox;
#[cfg(feature = "calendars")]
pub mod participant_identity;
pub mod principal;
pub mod push_subscription;
#[cfg(feature = "quota")]
pub mod quota;
pub mod share_notification;
#[cfg(feature = "mail")]
pub mod sieve;
#[cfg(feature = "mail")]
pub mod thread;
pub mod transport_reqwest;
#[cfg(feature = "mail")]
pub mod vacation_response;

pub use bytes::Bytes;

use crate::core::error::MethodError;
use crate::core::error::ProblemDetails;
use crate::core::set::SetError;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::Display;

#[cfg(feature = "websockets")]
pub mod client_ws;

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Hash, Clone)]
#[non_exhaustive]
pub enum DataType {
    // Core (always available)
    #[serde(rename = "Core")]
    Core,
    #[serde(rename = "PushSubscription")]
    PushSubscription,
    #[serde(rename = "Principal")]
    Principal,
    #[serde(rename = "ShareNotification")]
    ShareNotification,

    // Mail
    #[cfg(feature = "mail")]
    #[serde(rename = "Email")]
    Email,
    #[cfg(feature = "mail")]
    #[serde(rename = "EmailDelivery")]
    EmailDelivery,
    #[cfg(feature = "mail")]
    #[serde(rename = "EmailSubmission")]
    EmailSubmission,
    #[cfg(feature = "mail")]
    #[serde(rename = "Mailbox")]
    Mailbox,
    #[cfg(feature = "mail")]
    #[serde(rename = "Thread")]
    Thread,
    #[cfg(feature = "mail")]
    #[serde(rename = "Identity")]
    Identity,
    #[cfg(feature = "mail")]
    #[serde(rename = "SearchSnippet")]
    SearchSnippet,
    #[cfg(feature = "mail")]
    #[serde(rename = "VacationResponse")]
    VacationResponse,
    #[cfg(feature = "mail")]
    #[serde(rename = "MDN")]
    Mdn,
    #[cfg(feature = "mail")]
    #[serde(rename = "SieveScript")]
    SieveScript,

    // Calendars
    #[cfg(feature = "calendars")]
    #[serde(rename = "Calendar")]
    Calendar,
    #[cfg(feature = "calendars")]
    #[serde(rename = "CalendarEvent")]
    CalendarEvent,
    #[cfg(feature = "calendars")]
    #[serde(rename = "CalendarEventNotification")]
    CalendarEventNotification,
    #[cfg(feature = "calendars")]
    #[serde(rename = "ParticipantIdentity")]
    ParticipantIdentity,
    #[cfg(feature = "calendars")]
    #[serde(rename = "CalendarAlert")]
    CalendarAlert,

    // Contacts
    #[cfg(feature = "contacts")]
    #[serde(rename = "AddressBook")]
    AddressBook,
    #[cfg(feature = "contacts")]
    #[serde(rename = "ContactCard")]
    ContactCard,

    // Quota
    #[cfg(feature = "quota")]
    #[serde(rename = "Quota")]
    Quota,

    #[serde(rename = "FileNode")]
    FileNode,

    /// Unknown or feature-gated data type.
    #[serde(other)]
    Other,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "@type")]
#[non_exhaustive]
pub enum PushObject {
    StateChange {
        changed: HashMap<String, HashMap<DataType, String>>,
    },
    #[cfg(feature = "mail")]
    EmailPush {
        #[serde(rename = "accountId")]
        account_id: String,
        email: serde_json::Value,
    },
    #[cfg(feature = "calendars")]
    CalendarAlert(CalendarAlert),
    Group {
        entries: Vec<PushObject>,
    },
}

#[cfg(feature = "calendars")]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct CalendarAlert {
    #[serde(rename = "accountId")]
    pub account_id: String,
    #[serde(rename = "calendarEventId")]
    pub calendar_event_id: String,
    pub uid: String,
    #[serde(rename = "recurrenceId")]
    pub recurrence_id: Option<String>,
    #[serde(rename = "alertId")]
    pub alert_id: String,
}

#[derive(Debug, Clone)]
pub struct Get;
#[derive(Debug, Clone)]
pub struct Set;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
    /// Transport-level failure (network, TLS, timeout).
    Transport(core::transport::TransportError),
    /// JSON deserialization failure.
    Parse(serde_json::Error),
    /// Server returned an RFC 7807 problem details response.
    Problem(Box<ProblemDetails>),
    /// A JMAP method call returned an error response.
    Method(MethodError),
    /// A JMAP set operation returned per-object errors.
    Set(SetError<String>),
    /// Requested call ID not found in the response.
    CallNotFound(String),
    /// Requested object ID not found in set/copy/parse response.
    IdNotFound(String),
    /// Server returned an empty method response array.
    EmptyResponse,
    /// Not parsable as the expected format.
    NotParsable(String),
    /// URL template parsing failure.
    InvalidUrl(String),
    /// The session lists no primary account for the requested capability.
    NoPrimaryAccount {
        /// The capability URI that was looked up (e.g.
        /// `urn:ietf:params:jmap:mail`).
        capability: &'static str,
    },
    #[cfg(feature = "websockets")]
    /// WebSocket transport error.
    WebSocket(tokio_tungstenite::tungstenite::error::Error),
    #[cfg(feature = "websockets")]
    /// WebSocket connection not established.
    WebSocketNotConnected,
}

impl std::error::Error for Error {}

impl From<core::transport::TransportError> for Error {
    fn from(e: core::transport::TransportError) -> Self {
        if let Some(ref body) = e.body
            && let Ok(problem) = serde_json::from_slice::<ProblemDetails>(body)
        {
            return Error::Problem(Box::new(problem));
        }
        Error::Transport(e)
    }
}

impl From<serde_json::Error> for Error {
    fn from(e: serde_json::Error) -> Self {
        Error::Parse(e)
    }
}

impl From<MethodError> for Error {
    fn from(e: MethodError) -> Self {
        Error::Method(e)
    }
}

impl From<ProblemDetails> for Error {
    fn from(e: ProblemDetails) -> Self {
        Error::Problem(Box::new(e))
    }
}

impl From<SetError<String>> for Error {
    fn from(e: SetError<String>) -> Self {
        Error::Set(e)
    }
}

#[cfg(feature = "websockets")]
impl From<tokio_tungstenite::tungstenite::error::Error> for Error {
    fn from(e: tokio_tungstenite::tungstenite::error::Error) -> Self {
        Error::WebSocket(e)
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Transport(e) => write!(f, "Transport error: {e}"),
            Error::Parse(e) => write!(f, "Parse error: {e}"),
            Error::Problem(e) => write!(f, "Problem: {e}"),
            Error::Method(e) => write!(f, "Method error: {e}"),
            Error::Set(e) => write!(f, "Set error: {e}"),
            Error::CallNotFound(id) => write!(f, "Call {id} not found in response"),
            Error::IdNotFound(id) => write!(f, "Id {id} not found"),
            Error::EmptyResponse => write!(f, "Server returned no results"),
            Error::NotParsable(id) => write!(f, "{id} is not parsable"),
            Error::InvalidUrl(msg) => write!(f, "Invalid URL: {msg}"),
            Error::NoPrimaryAccount { capability } => write!(
                f,
                "Session lists no primary account for capability {capability}"
            ),
            #[cfg(feature = "websockets")]
            Error::WebSocket(e) => write!(f, "WebSocket error: {e}"),
            #[cfg(feature = "websockets")]
            Error::WebSocketNotConnected => write!(f, "WebSocket connection not established"),
        }
    }
}

impl Display for DataType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DataType::Core => write!(f, "Core"),
            DataType::PushSubscription => write!(f, "PushSubscription"),
            DataType::Principal => write!(f, "Principal"),
            DataType::ShareNotification => write!(f, "ShareNotification"),
            DataType::FileNode => write!(f, "FileNode"),
            #[cfg(feature = "mail")]
            DataType::Email => write!(f, "Email"),
            #[cfg(feature = "mail")]
            DataType::EmailDelivery => write!(f, "EmailDelivery"),
            #[cfg(feature = "mail")]
            DataType::EmailSubmission => write!(f, "EmailSubmission"),
            #[cfg(feature = "mail")]
            DataType::Mailbox => write!(f, "Mailbox"),
            #[cfg(feature = "mail")]
            DataType::Thread => write!(f, "Thread"),
            #[cfg(feature = "mail")]
            DataType::Identity => write!(f, "Identity"),
            #[cfg(feature = "mail")]
            DataType::SearchSnippet => write!(f, "SearchSnippet"),
            #[cfg(feature = "mail")]
            DataType::VacationResponse => write!(f, "VacationResponse"),
            #[cfg(feature = "mail")]
            DataType::Mdn => write!(f, "MDN"),
            #[cfg(feature = "mail")]
            DataType::SieveScript => write!(f, "SieveScript"),
            #[cfg(feature = "calendars")]
            DataType::Calendar => write!(f, "Calendar"),
            #[cfg(feature = "calendars")]
            DataType::CalendarEvent => write!(f, "CalendarEvent"),
            #[cfg(feature = "calendars")]
            DataType::CalendarEventNotification => write!(f, "CalendarEventNotification"),
            #[cfg(feature = "calendars")]
            DataType::ParticipantIdentity => write!(f, "ParticipantIdentity"),
            #[cfg(feature = "calendars")]
            DataType::CalendarAlert => write!(f, "CalendarAlert"),
            #[cfg(feature = "contacts")]
            DataType::AddressBook => write!(f, "AddressBook"),
            #[cfg(feature = "contacts")]
            DataType::ContactCard => write!(f, "ContactCard"),
            #[cfg(feature = "quota")]
            DataType::Quota => write!(f, "Quota"),
            DataType::Other => write!(f, "Other"),
        }
    }
}

#[cfg(test)]
mod tests;
