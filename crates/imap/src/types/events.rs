//! Application-facing event impact helpers.

use super::{FetchResponse, MailboxInfo, ResponseCode, UidRange};
use crate::TypedEvent;

/// What a consumer should generally do after receiving an asynchronous event.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum EventImpact {
    /// No immediate sync action is required.
    None,
    /// Present an alert to the user.
    Alert(String),
    /// Capabilities changed; refresh local feature decisions.
    CapabilitiesChanged,
    /// Server-level metadata annotations changed.
    ServerMetadataChanged,
    /// The selected mailbox changed and should be refreshed.
    SelectedMailboxChanged,
    /// The selected mailbox has an expunge-style change where a full UID
    /// reconciliation is safer than applying a local delta.
    SelectedMailboxResync,
    /// A non-selected mailbox changed.
    MailboxChanged(MailboxInfo),
    /// Messages vanished by UID.
    UidsVanished {
        /// `true` for `VANISHED (EARLIER)`.
        earlier: bool,
        /// UID ranges returned by the server.
        uids: Vec<UidRange>,
    },
    /// Message metadata changed for a selected mailbox message.
    FetchUpdate(Box<FetchResponse>),
    /// The event queue overflowed; the caller should resync any state derived
    /// from asynchronous events.
    EventQueueOverflow { dropped_count: usize },
    /// NOTIFY overflowed; the registration was cleared and should be rebuilt
    /// if the caller still wants notifications.
    NotifyRegistrationLost,
    /// The server is closing the connection.
    ConnectionClosing {
        /// Optional server response code.
        code: Option<ResponseCode>,
        /// Human-readable server text.
        text: String,
    },
    /// Extension-defined event. Consumers that care about the extension should
    /// inspect the raw typed event.
    Extension,
}

impl TypedEvent {
    /// Classify an event into a consumer-facing sync impact.
    pub fn impact(&self) -> EventImpact {
        match self {
            Self::Alert(text) => EventImpact::Alert(text.clone()),
            Self::Bye { code, text } => EventImpact::ConnectionClosing {
                code: code.clone(),
                text: text.clone(),
            },
            Self::NotificationOverflow { .. } => EventImpact::NotifyRegistrationLost,
            Self::QueueOverflow { dropped_count, .. } => EventImpact::EventQueueOverflow {
                dropped_count: *dropped_count,
            },
            Self::CapabilityChange(_) => EventImpact::CapabilitiesChanged,
            Self::Exists(_) => EventImpact::SelectedMailboxChanged,
            Self::Recent(_) => EventImpact::None,
            Self::Expunge(_) => EventImpact::SelectedMailboxResync,
            Self::Vanished { earlier, uids } => EventImpact::UidsVanished {
                earlier: *earlier,
                uids: uids.clone(),
            },
            Self::FetchUpdate(fetch) => EventImpact::FetchUpdate(fetch.clone()),
            Self::MailboxEvent(info) => EventImpact::MailboxChanged(info.clone()),
            Self::MetadataChange { .. } => EventImpact::SelectedMailboxChanged,
            Self::ServerMetadataChange { .. } => EventImpact::ServerMetadataChanged,
            Self::Extension(_) => EventImpact::Extension,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{Capability, UntaggedResponse};

    #[test]
    fn impact_classifies_typed_events() {
        let fetch = Box::new(FetchResponse {
            seq: 7,
            ..Default::default()
        });
        let mailbox = MailboxInfo::default();
        let vanished = vec![UidRange::range(10, 12)];

        let cases = vec![
            (
                TypedEvent::Alert("notice".to_owned()),
                EventImpact::Alert("notice".to_owned()),
            ),
            (
                TypedEvent::Bye {
                    code: Some(ResponseCode::Unavailable),
                    text: "bye".to_owned(),
                },
                EventImpact::ConnectionClosing {
                    code: Some(ResponseCode::Unavailable),
                    text: "bye".to_owned(),
                },
            ),
            (
                TypedEvent::NotificationOverflow {
                    code: None,
                    text: "overflow".to_owned(),
                },
                EventImpact::NotifyRegistrationLost,
            ),
            (
                TypedEvent::QueueOverflow {
                    dropped_count: 3,
                    since: std::time::Instant::now(),
                },
                EventImpact::EventQueueOverflow { dropped_count: 3 },
            ),
            (
                TypedEvent::CapabilityChange(vec![Capability::Idle]),
                EventImpact::CapabilitiesChanged,
            ),
            (TypedEvent::Exists(2), EventImpact::SelectedMailboxChanged),
            (TypedEvent::Recent(1), EventImpact::None),
            (TypedEvent::Expunge(1), EventImpact::SelectedMailboxResync),
            (
                TypedEvent::Vanished {
                    earlier: true,
                    uids: vanished.clone(),
                },
                EventImpact::UidsVanished {
                    earlier: true,
                    uids: vanished,
                },
            ),
            (
                TypedEvent::FetchUpdate(fetch.clone()),
                EventImpact::FetchUpdate(fetch),
            ),
            (
                TypedEvent::MailboxEvent(mailbox.clone()),
                EventImpact::MailboxChanged(mailbox),
            ),
            (
                TypedEvent::MetadataChange {},
                EventImpact::SelectedMailboxChanged,
            ),
            (
                TypedEvent::ServerMetadataChange {},
                EventImpact::ServerMetadataChanged,
            ),
            (
                TypedEvent::Extension(Box::new(UntaggedResponse::Search {
                    uids: Vec::new(),
                    mod_seq: None,
                })),
                EventImpact::Extension,
            ),
        ];

        for (event, impact) in cases {
            assert_eq!(event.impact(), impact);
        }
    }
}
