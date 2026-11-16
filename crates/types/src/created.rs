//! The success of a single-request create.

use crate::{AccountError, DiagnosticText, Warning, WarningKind};

/// What an acknowledged create door (`draft_create`, `container_create`,
/// `filter_create`, `contact_create`, `event_create`) returns.
///
/// Holding a `Created` means the server acknowledged the create: the object
/// exists. It never means the outcome is uncertain - an unanswered item, a
/// contradictory answer or an unreadable response stays an `Err`.
///
/// The identity is separate from the creation. A server may acknowledge a
/// create without naming the new object (IMAP without APPENDUID, as RFC 4315
/// requires for a `UIDNOTSTICKY` mailbox; a provider answer whose id is absent
/// or empty). That is still a success: an enterprise Outlook user who saves a
/// draft that was saved expects "saved", never a failure that invites saving
/// it again. Without an id the caller cannot address the object - no
/// `draft_update`, `draft_discard` or other id-taking call for it - and cannot
/// tell which object a later sync surfaces is this one; creating it again may
/// duplicate it. An unidentified `Created` always carries at least one
/// [`WarningKind::CreatedWithoutIdentity`] warning saying why.
///
/// The provider boundary validates the native id before wrapping it: an
/// empty native id is unidentified, never an id that names nothing.
///
/// Warnings may also accompany an identified create, for an ancillary leg that
/// degraded after the object was created. They are advisory, never an
/// instruction to retry the create.
#[derive(Debug, Clone)]
#[non_exhaustive]
#[must_use]
pub struct Created<T> {
    id: Option<T>,
    warnings: Vec<Warning>,
}

impl<T> Created<T> {
    /// A create the server acknowledged and identified.
    pub fn identified(id: T) -> Self {
        Self {
            id: Some(id),
            warnings: Vec::new(),
        }
    }

    /// A create the server acknowledged without a usable identity.
    /// `warning` says why; its kind is normally
    /// [`WarningKind::CreatedWithoutIdentity`] (see [`Self::unidentified_because`]).
    pub fn unidentified(warning: Warning) -> Self {
        Self {
            id: None,
            warnings: vec![warning],
        }
    }

    /// An unidentified create explained by a [`WarningKind::CreatedWithoutIdentity`]
    /// warning carrying `message`, and `evidence` (the provider's
    /// contract-violation leaf) when the answer broke the provider's contract.
    /// Pass no evidence when the protocol permits an anonymous answer.
    pub fn unidentified_because(message: DiagnosticText, evidence: Option<AccountError>) -> Self {
        let mut warning = Warning::new(WarningKind::CreatedWithoutIdentity, message);
        warning.error = evidence;
        Self::unidentified(warning)
    }

    /// Identified when `id` is `Some`, else [`Self::unidentified_because`].
    pub fn from_option(
        id: Option<T>,
        message: impl FnOnce() -> DiagnosticText,
        evidence: impl FnOnce() -> Option<AccountError>,
    ) -> Self {
        match id {
            Some(id) => Self::identified(id),
            None => Self::unidentified_because(message(), evidence()),
        }
    }

    /// Attach an advisory warning.
    pub fn with_warning(mut self, warning: Warning) -> Self {
        self.warnings.push(warning);
        self
    }

    /// The new object's id, when the server named it.
    #[must_use]
    pub fn id(&self) -> Option<&T> {
        self.id.as_ref()
    }

    /// The new object's id, dropping the warnings.
    #[must_use]
    pub fn into_id(self) -> Option<T> {
        self.id
    }

    /// Advisory warnings, in the order they were attached.
    #[must_use]
    pub fn warnings(&self) -> &[Warning] {
        &self.warnings
    }

    /// The id and the warnings.
    #[must_use]
    pub fn into_parts(self) -> (Option<T>, Vec<Warning>) {
        (self.id, self.warnings)
    }

    /// Map the identity, keeping the warnings.
    pub fn map<U>(self, f: impl FnOnce(T) -> U) -> Created<U> {
        Created {
            id: self.id.map(f),
            warnings: self.warnings,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unidentified_always_explains_itself() {
        let created: Created<String> =
            Created::unidentified_because(DiagnosticText::support_only("no id"), None);
        assert!(created.id().is_none());
        assert_eq!(created.warnings().len(), 1);
        assert_eq!(
            created.warnings()[0].kind,
            WarningKind::CreatedWithoutIdentity
        );
    }

    #[test]
    fn identified_carries_no_warning_and_maps() {
        let created = Created::identified(7_u32).map(|n| n.to_string());
        assert_eq!(created.id().map(String::as_str), Some("7"));
        assert!(created.warnings().is_empty());
    }
}
