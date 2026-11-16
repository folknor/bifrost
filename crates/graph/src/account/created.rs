//! The one rule every Graph create door applies to the identity a
//! successful create answer carries.

use bifrost_types::{AccountOperation, Created, DiagnosticText, ProtocolErrorKind};

use super::graph_error::protocol_violation;

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
                detail,
            ))
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;

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
