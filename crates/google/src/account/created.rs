//! The one rule every Google create door applies to the identity a
//! successful create answer carries.

use bifrost_types::{Created, DiagnosticText};

use super::error::{GmailErrorContext, into_account_error};

/// `Created` from the native id of a decoded create-success answer.
///
/// The answer proves the create applied, so a missing or empty id is an
/// unidentified success, never a failure: an enterprise user whose save
/// worked expects it reported as saved, and a failure would invite a
/// duplicating retry. The id is still a provider contract violation, so the
/// warning carries the `Protocol(MissingField)` leaf (acknowledged evidence)
/// that names `field`. An empty id is treated as absent: it would be a handle
/// that names nothing.
pub(crate) fn created_or_unidentified(
    id: Option<String>,
    field: &'static str,
    detail: &'static str,
    ctx: GmailErrorContext,
) -> Created<String> {
    Created::from_option(
        id.filter(|id| !id.is_empty()),
        || DiagnosticText::support_only(detail),
        || {
            Some(into_account_error(
                crate::error::Error::missing_field(field, detail),
                ctx,
            ))
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use bifrost_types::AccountOperation;

    #[test]
    fn an_empty_id_is_unidentified_like_an_absent_one() {
        for id in [None, Some(String::new())] {
            let created = created_or_unidentified(
                id,
                "id",
                "no id",
                GmailErrorContext::base(AccountOperation::FilterCreate),
            );
            assert!(created.id().is_none());
            let [warning] = created.warnings() else {
                panic!("one warning explains the missing identity");
            };
            assert_eq!(
                warning
                    .error
                    .as_ref()
                    .map(bifrost_types::AccountError::kind),
                Some(&bifrost_types::AccountErrorKind::Protocol(
                    bifrost_types::ProtocolErrorKind::MissingField
                ))
            );
        }
        let created = created_or_unidentified(
            Some("f1".into()),
            "id",
            "no id",
            GmailErrorContext::base(AccountOperation::FilterCreate),
        );
        assert_eq!(created.id().map(String::as_str), Some("f1"));
    }
}
