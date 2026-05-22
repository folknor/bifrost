use bifrost_types::{
    ChangeCursor, CursorScope, Error as AccountError, OpaqueChangeState, ProtocolKind,
};
use serde::{Deserialize, Serialize};

pub(crate) const GMAIL_PROTOCOL: ProtocolKind = ProtocolKind::Gmail;
pub(crate) const GMAIL_ENVELOPE_VERSION: u32 = 1;
const GMAIL_SCHEMA_VERSION: u8 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct GmailChangeState {
    pub history_id: u64,
    pub profile_email: String,
    pub schema_version: u8,
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
    let bytes = serde_json::to_vec(state).unwrap_or_default();
    OpaqueChangeState {
        protocol: GMAIL_PROTOCOL,
        envelope_version: GMAIL_ENVELOPE_VERSION,
        bytes,
    }
}

pub(crate) fn decode_gmail_state(
    state: &OpaqueChangeState,
) -> Result<GmailChangeState, AccountError> {
    if state.protocol != GMAIL_PROTOCOL {
        return Err(AccountError::SchemaIncompatible);
    }
    if state.envelope_version != GMAIL_ENVELOPE_VERSION {
        return Err(AccountError::SchemaIncompatible);
    }
    let decoded: GmailChangeState =
        serde_json::from_slice(&state.bytes).map_err(|err| AccountError::Other(err.to_string()))?;
    if decoded.schema_version != GMAIL_SCHEMA_VERSION {
        return Err(AccountError::SchemaIncompatible);
    }
    Ok(decoded)
}

pub(crate) fn decode_gmail_state_for_profile(
    state: &OpaqueChangeState,
    profile_email: &str,
) -> Result<GmailChangeState, AccountError> {
    let decoded = decode_gmail_state(state)?;
    if decoded.profile_email != profile_email {
        return Err(AccountError::Other(format!(
            "gmail cursor belongs to {}, opened account is {profile_email}",
            decoded.profile_email
        )));
    }
    Ok(decoded)
}

pub(crate) fn cursor_from_state(state: OpaqueChangeState) -> ChangeCursor {
    ChangeCursor {
        scope: CursorScope::Account,
        server_state: state,
        advanced_through: None,
        envelope_version: GMAIL_ENVELOPE_VERSION,
    }
}

pub(crate) fn cursor_for_history(history_id: u64, profile_email: &str) -> ChangeCursor {
    cursor_from_state(encode_gmail_state(&GmailChangeState::new(
        history_id,
        profile_email,
    )))
}

#[cfg(test)]
mod tests {
    use bifrost_types::ProtocolKind;

    use super::*;

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
        assert!(matches!(
            decode_gmail_state(&opaque),
            Err(AccountError::SchemaIncompatible)
        ));
    }

    #[test]
    fn rejects_envelope_version_mismatch() {
        let mut opaque = encode_gmail_state(&GmailChangeState::new(1, "a@example.test"));
        opaque.envelope_version = 2;
        assert!(matches!(
            decode_gmail_state(&opaque),
            Err(AccountError::SchemaIncompatible)
        ));
    }

    #[test]
    fn rejects_profile_swap() {
        let opaque = encode_gmail_state(&GmailChangeState::new(1, "a@example.test"));
        let err =
            decode_gmail_state_for_profile(&opaque, "b@example.test").expect_err("profile swap");
        assert!(matches!(err, AccountError::Other(_)));
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
    fn rejects_schema_version_mismatch() {
        // Synthesize a payload with a future schema version.
        let payload = serde_json::json!({
            "history_id": 7u64,
            "profile_email": "a@example.test",
            "schema_version": GMAIL_SCHEMA_VERSION + 1,
        });
        let opaque = OpaqueChangeState {
            protocol: GMAIL_PROTOCOL,
            envelope_version: GMAIL_ENVELOPE_VERSION,
            bytes: serde_json::to_vec(&payload).expect("serialize"),
        };
        assert!(matches!(
            decode_gmail_state(&opaque),
            Err(AccountError::SchemaIncompatible)
        ));
    }

    #[test]
    fn rejects_truncated_bytes() {
        let mut opaque = encode_gmail_state(&GmailChangeState::new(1, "a@example.test"));
        // Truncate to a definitely-invalid prefix.
        opaque.bytes.truncate(opaque.bytes.len() / 2);
        let err = decode_gmail_state(&opaque).expect_err("truncated bytes");
        assert!(matches!(err, AccountError::Other(_)));
    }

    #[test]
    fn rejects_empty_bytes() {
        let opaque = OpaqueChangeState {
            protocol: GMAIL_PROTOCOL,
            envelope_version: GMAIL_ENVELOPE_VERSION,
            bytes: Vec::new(),
        };
        let err = decode_gmail_state(&opaque).expect_err("empty bytes");
        assert!(matches!(err, AccountError::Other(_)));
    }

    #[test]
    fn cursor_for_history_round_trips_via_envelope() {
        let cursor = cursor_for_history(99, "a@example.test");
        assert_eq!(cursor.scope, CursorScope::Account);
        assert_eq!(cursor.envelope_version, GMAIL_ENVELOPE_VERSION);
        let decoded =
            decode_gmail_state_for_profile(&cursor.server_state, "a@example.test").expect("decode");
        assert_eq!(decoded.history_id, 99);
    }
}
