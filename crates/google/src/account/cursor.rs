//! Gmail cursor envelope: encode, decode, and identity check.
//!
//! Decode failures return crate-internal `GmailLocalError` variants;
//! the account translation boundary maps these to
//! `SyncState(SchemaIncompatible | CursorInvalid)` with the cursor
//! scope so the central recovery mapper derives
//! `Engine(SchemaIncompatible)` or
//! `Engine(RestartScope(CursorScope::Account))`.

use bifrost_types::{ChangeCursor, CursorScope, OpaqueChangeState, ProtocolKind};
use serde::{Deserialize, Serialize};

use bifrost_types::CursorDecodeFault;

use crate::error::{Error, GmailLocalError};

/// Gmail inventory samples its history cursor before listing, so any deletion
/// after that point is carried by the change stream. Users expect a
/// concurrently deleted message to disappear without repair debt.
///
/// Two premises, only one of which the cursor supplies. The cursor ordering
/// bridges a SUBSEQUENT deletion; it does not establish why the GET said
/// not-found. That a `users.messages.get` 404 is authoritative current absence
/// in the same mailbox is a provider assumption, not a consequence of the
/// cursor (mailbox identity across a walk rests on the same unbound token
/// source as every other request; see the changes-poll identity check).
///
/// This proof is only for inventory and its repair lane, not mutation success.
pub(super) fn absence_proven_by_cursor_bridge(
    error: &bifrost_types::AccountError,
    message_id: &str,
) -> Option<bifrost_types::DefinitiveIrrelevance> {
    matches!(
        error.kind(),
        bifrost_types::AccountErrorKind::NotFound(bifrost_types::ResourceKind::Message)
    )
    .then(
        || bifrost_types::DefinitiveIrrelevance::AbsentUnderCursorBridge {
            detail: format!(
                "Gmail message {message_id} absent; the history cursor precedes the \
                 inventory walk, so the change stream bridges any removal"
            ),
        },
    )
}

pub(crate) const GMAIL_PROTOCOL: ProtocolKind = ProtocolKind::Gmail;
pub(crate) const GMAIL_ENVELOPE_VERSION: u32 = 1;
const GMAIL_SCHEMA_VERSION: u8 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct GmailChangeState {
    pub(crate) history_id: u64,
    pub(crate) profile_email: String,
    pub(crate) schema_version: u8,
}

impl GmailChangeState {
    pub(crate) fn new(history_id: u64, profile_email: impl Into<String>) -> Self {
        Self {
            history_id,
            profile_email: profile_email.into(),
            schema_version: GMAIL_SCHEMA_VERSION,
        }
    }
}

pub(crate) fn encode_gmail_state(state: &GmailChangeState) -> OpaqueChangeState {
    let bytes = serde_json::to_vec(state).expect("GmailChangeState must serialize");
    OpaqueChangeState {
        protocol: GMAIL_PROTOCOL,
        envelope_version: bifrost_types::PayloadVersion::new(GMAIL_ENVELOPE_VERSION),
        bytes,
    }
}

pub(crate) fn decode_gmail_state(state: &OpaqueChangeState) -> Result<GmailChangeState, Error> {
    let bytes = state
        .open(
            GMAIL_PROTOCOL,
            bifrost_types::PayloadVersion::new(GMAIL_ENVELOPE_VERSION),
        )
        .map_err(|fault| {
            let detail = format!("Gmail cursor header: {fault:?}");
            cursor_error(fault, detail)
        })?;
    // Read the payload's schema discriminator before the layout it selects:
    // a newer schema may reshape the other fields, and must still report as
    // a future version rather than as malformed bytes.
    let probe: SchemaProbe = serde_json::from_slice(bytes).map_err(malformed)?;
    match probe.schema_version.cmp(&GMAIL_SCHEMA_VERSION) {
        std::cmp::Ordering::Equal => {}
        direction => {
            let fault = if direction == std::cmp::Ordering::Greater {
                bifrost_types::CursorDecodeFault::FutureVersion
            } else {
                bifrost_types::CursorDecodeFault::RetiredVersion
            };
            return Err(cursor_error(
                fault,
                format!(
                    "expected schema version {GMAIL_SCHEMA_VERSION}, got {}",
                    probe.schema_version
                ),
            ));
        }
    }
    serde_json::from_slice(bytes).map_err(malformed)
}

/// The schema discriminator alone; serde ignores the remaining fields.
#[derive(Deserialize)]
struct SchemaProbe {
    schema_version: u8,
}

fn malformed(err: serde_json::Error) -> Error {
    cursor_error(
        bifrost_types::CursorDecodeFault::Malformed,
        format!("cursor JSON decode failed: {err}"),
    )
}

pub(crate) fn decode_gmail_state_for_profile(
    state: &OpaqueChangeState,
    profile_email: &str,
) -> Result<GmailChangeState, Error> {
    let decoded = decode_gmail_state(state)?;
    validate_gmail_profile(&decoded, profile_email)?;
    Ok(decoded)
}

pub(super) fn validate_gmail_profile(
    decoded: &GmailChangeState,
    profile_email: &str,
) -> Result<(), Error> {
    if !super::mailbox::MailboxAddress::from_provider_value(profile_email)
        .matches(&decoded.profile_email)
    {
        return Err(Error::Local(GmailLocalError::AccountIdentityMismatch {
            cursor_email: decoded.profile_email.clone(),
            profile_email: profile_email.to_string(),
        }));
    }
    Ok(())
}

pub(crate) fn cursor_from_state(state: OpaqueChangeState) -> ChangeCursor {
    ChangeCursor {
        scope: CursorScope::Account,
        server_state: state,
        advanced_through: None,
        envelope_version: bifrost_types::TraitEnvelopeVersion::new(
            bifrost_types::CHANGE_CURSOR_ENVELOPE_VERSION,
        ),
    }
}

pub(crate) fn cursor_for_history(history_id: u64, profile_email: &str) -> ChangeCursor {
    cursor_from_state(encode_gmail_state(&GmailChangeState::new(
        history_id,
        profile_email,
    )))
}

fn cursor_error(fault: CursorDecodeFault, detail: String) -> Error {
    Error::Local(GmailLocalError::InvalidCursor { fault, detail })
}

#[cfg(test)]
mod tests {
    use bifrost_types::ProtocolKind;

    use super::*;

    fn assert_cursor_kind(err: Error, kind: CursorDecodeFault) {
        match err {
            Error::Local(GmailLocalError::InvalidCursor { fault, .. }) => {
                assert_eq!(fault, kind);
            }
            other => panic!("expected cursor error, got {other:?}"),
        }
    }

    #[test]
    fn cursor_envelope_round_trips() {
        let state = GmailChangeState::new(12345, "a@example.test");
        let opaque = encode_gmail_state(&state);
        assert_eq!(decode_gmail_state(&opaque).expect("decode"), state);
        let cursor = cursor_from_state(opaque);
        assert_eq!(cursor.scope, CursorScope::Account);
        assert!(cursor.advanced_through.is_none());
    }

    #[test]
    fn rejects_wrong_protocol() {
        let mut opaque = encode_gmail_state(&GmailChangeState::new(1, "a@example.test"));
        opaque.protocol = ProtocolKind::Jmap;
        let err = decode_gmail_state(&opaque).expect_err("wrong protocol");
        assert_cursor_kind(err, CursorDecodeFault::ProtocolMismatch);
    }

    #[test]
    fn rejects_envelope_version_mismatch_in_its_direction() {
        let mut opaque = encode_gmail_state(&GmailChangeState::new(1, "a@example.test"));
        opaque.envelope_version = bifrost_types::PayloadVersion::new(GMAIL_ENVELOPE_VERSION + 1);
        let err = decode_gmail_state(&opaque).expect_err("future envelope");
        assert_cursor_kind(err, CursorDecodeFault::FutureVersion);
        opaque.envelope_version = bifrost_types::PayloadVersion::new(GMAIL_ENVELOPE_VERSION - 1);
        let err = decode_gmail_state(&opaque).expect_err("retired envelope");
        assert_cursor_kind(err, CursorDecodeFault::RetiredVersion);
    }

    /// A newer schema may reshape every other field; the discriminator is
    /// read first, so it still reports as a future version.
    #[test]
    fn a_reshaped_future_schema_is_a_future_version_not_malformed() {
        let payload = serde_json::json!({
            "schema_version": GMAIL_SCHEMA_VERSION + 1,
            "history": { "id": "7" },
        });
        let opaque = OpaqueChangeState {
            protocol: GMAIL_PROTOCOL,
            envelope_version: bifrost_types::PayloadVersion::new(GMAIL_ENVELOPE_VERSION),
            bytes: serde_json::to_vec(&payload).expect("serialize"),
        };
        let err = decode_gmail_state(&opaque).expect_err("future schema");
        assert_cursor_kind(err, CursorDecodeFault::FutureVersion);
    }

    #[test]
    fn rejects_profile_swap() {
        let opaque = encode_gmail_state(&GmailChangeState::new(1, "a@example.test"));
        let err =
            decode_gmail_state_for_profile(&opaque, "b@example.test").expect_err("profile swap");
        assert!(matches!(
            err,
            Error::Local(GmailLocalError::AccountIdentityMismatch { .. })
        ));
    }

    #[test]
    fn accepts_matching_profile() {
        let opaque = encode_gmail_state(&GmailChangeState::new(42, "a@example.test"));
        let decoded =
            decode_gmail_state_for_profile(&opaque, "a@example.test").expect("matching profile");
        assert_eq!(decoded.history_id, 42);
        assert_eq!(decoded.profile_email, "a@example.test");
        assert_eq!(decoded.schema_version, GMAIL_SCHEMA_VERSION);
    }

    #[test]
    fn accepts_matching_profile_with_different_ascii_case() {
        let opaque = encode_gmail_state(&GmailChangeState::new(42, "Person@Example.Test"));
        let decoded = decode_gmail_state_for_profile(&opaque, "person@example.test")
            .expect("email identity comparison is case-insensitive");
        assert_eq!(decoded.history_id, 42);
    }

    #[test]
    fn rejects_schema_version_mismatch() {
        let payload = serde_json::json!({
            "history_id": 7u64,
            "profile_email": "a@example.test",
            "schema_version": GMAIL_SCHEMA_VERSION + 1,
        });
        let opaque = OpaqueChangeState {
            protocol: GMAIL_PROTOCOL,
            envelope_version: bifrost_types::PayloadVersion::new(GMAIL_ENVELOPE_VERSION),
            bytes: serde_json::to_vec(&payload).expect("serialize"),
        };
        let err = decode_gmail_state(&opaque).expect_err("schema mismatch");
        assert_cursor_kind(err, CursorDecodeFault::FutureVersion);
    }

    #[test]
    fn rejects_truncated_bytes() {
        let mut opaque = encode_gmail_state(&GmailChangeState::new(1, "a@example.test"));
        opaque.bytes.truncate(opaque.bytes.len() / 2);
        let err = decode_gmail_state(&opaque).expect_err("truncated bytes");
        assert_cursor_kind(err, CursorDecodeFault::Malformed);
    }

    #[test]
    fn rejects_empty_bytes() {
        let opaque = OpaqueChangeState {
            protocol: GMAIL_PROTOCOL,
            envelope_version: bifrost_types::PayloadVersion::new(GMAIL_ENVELOPE_VERSION),
            bytes: Vec::new(),
        };
        let err = decode_gmail_state(&opaque).expect_err("empty bytes");
        assert_cursor_kind(err, CursorDecodeFault::Malformed);
    }

    #[test]
    fn cursor_for_history_round_trips_via_envelope() {
        let cursor = cursor_for_history(99, "a@example.test");
        assert_eq!(cursor.scope, CursorScope::Account);
        assert_eq!(
            cursor.envelope_version.get(),
            bifrost_types::CHANGE_CURSOR_ENVELOPE_VERSION
        );
        let decoded =
            decode_gmail_state_for_profile(&cursor.server_state, "a@example.test").expect("decode");
        assert_eq!(decoded.history_id, 99);
    }
}
