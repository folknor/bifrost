//! The one rule every Graph create door applies to the identity a
//! successful create answer carries.

use bifrost_types::{
    AccountOperation, AttemptCause, Created, DiagnosticText, ProtocolErrorKind, TransmissionState,
};

use super::graph_error::protocol_violation;

/// The part of a create-success answer a create door reads: its id.
///
/// A door decodes its answer into this rather than into the full resource
/// type, so an answer that omits the id, or a field the door never reads,
/// still reports the save that happened instead of failing to decode. The
/// answer must still be a JSON object, and a present id must be a string or
/// `null`: anything else is a success this client cannot read, and stays the
/// unreadable-success `Protocol(ParseFailed)` error of a body that fails to
/// decode.
#[derive(Debug)]
pub(crate) struct CreateAnswer {
    pub(crate) id: Option<String>,
}

impl<'de> serde::Deserialize<'de> for CreateAnswer {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(serde::Deserialize)]
        struct Fields {
            #[serde(default)]
            id: Option<String>,
        }
        // Through a map first: a derived struct also accepts a JSON array,
        // which would read `[]` as an object without an id.
        let map = serde_json::Map::<String, serde_json::Value>::deserialize(deserializer)?;
        let fields = Fields::deserialize(serde_json::Value::Object(map))
            .map_err(serde::de::Error::custom)?;
        Ok(Self { id: fields.id })
    }
}

/// `Created` from the native id of a decoded create-success answer.
///
/// The answer proves the create applied, so a missing or empty id is an
/// unidentified success, never a failure: an enterprise user whose save
/// worked expects it reported as saved, and a failure would invite a
/// duplicating retry. The id is still a Graph contract violation, so the
/// warning carries that `Protocol(ContractViolation)` leaf. An empty id is
/// treated as absent: it would be a handle that names nothing.
pub(crate) fn created_or_unidentified(
    id: Option<String>,
    operation: AccountOperation,
    detail: &'static str,
) -> Created<String> {
    Created::from_option(
        id.filter(|id| !id.is_empty()),
        || DiagnosticText::support_only(detail),
        || {
            Some(protocol_violation(
                ProtocolErrorKind::ContractViolation,
                operation,
                None,
                // The create was answered with success; only its id is unusable.
                AttemptCause::new(TransmissionState::Acknowledged),
                detail,
            ))
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_create_answer_reads_only_an_object_id() {
        let decode = |raw: &str| serde_json::from_str::<CreateAnswer>(raw).map(|a| a.id);
        assert_eq!(decode(r#"{"id":"f1"}"#).unwrap().as_deref(), Some("f1"));
        // Fields the door never reads cannot turn a saved object into a failure.
        assert_eq!(
            decode(r#"{"id":"e1","start":42}"#).unwrap().as_deref(),
            Some("e1")
        );
        assert_eq!(decode("{}").unwrap(), None);
        assert_eq!(decode(r#"{"id":null}"#).unwrap(), None);
        for unreadable in [r#"{"id":7}"#, "[]", r#"["x"]"#, "\"x\"", "null"] {
            assert!(decode(unreadable).is_err(), "{unreadable} must not decode");
        }
    }

    #[test]
    fn the_violation_is_acknowledged_evidence() {
        let created = created_or_unidentified(None, AccountOperation::ContainerCreate, "no id");
        let error = created.warnings()[0].error.as_ref().expect("leaf");
        assert!(error.chain().iter().any(|cause| matches!(
            cause,
            bifrost_types::Cause::Attempt(a)
                if a.transmission_state == TransmissionState::Acknowledged
        )));
    }

    #[test]
    fn an_absent_or_empty_id_is_an_unidentified_success_with_the_violation() {
        for id in [None, Some(String::new())] {
            let created = created_or_unidentified(id, AccountOperation::DraftCreate, "no id");
            assert!(created.id().is_none());
            let [warning] = created.warnings() else {
                panic!("one warning explains the missing identity");
            };
            assert_eq!(
                warning.kind,
                bifrost_types::WarningKind::CreatedWithoutIdentity
            );
            assert_eq!(
                warning
                    .error
                    .as_ref()
                    .map(bifrost_types::AccountError::kind),
                Some(&bifrost_types::AccountErrorKind::Protocol(
                    ProtocolErrorKind::ContractViolation
                ))
            );
        }
        let created =
            created_or_unidentified(Some("m1".into()), AccountOperation::DraftCreate, "no id");
        assert_eq!(created.id().map(String::as_str), Some("m1"));
        assert!(created.warnings().is_empty());
    }
}
