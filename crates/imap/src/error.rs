//! IMAP error types.

use std::io::Error as IoError;
use std::str::Utf8Error;

use base64::DecodeError;

/// A convenience wrapper around `Result` for `imap::Error`.
pub type Result<T> = std::result::Result<T, Error>;

/// A set of errors that can occur in the IMAP client
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    /// An `io::Error` that occurred while trying to read or write to a network stream.
    #[error("io: {0}")]
    Io(#[from] IoError),
    /// A BAD response from the IMAP server.
    #[error("bad response: {0}")]
    Bad(String),
    /// A NO response from the IMAP server.
    #[error("no response: {0}")]
    No(String),
    /// The connection was terminated unexpectedly.
    #[error("connection lost")]
    ConnectionLost,
    /// Error parsing a server response.
    #[error("parse: {0}")]
    Parse(#[from] ParseError),
    /// Command inputs were not valid [IMAP
    /// strings](https://tools.ietf.org/html/rfc3501#section-4.3).
    #[error("validate: {0}")]
    Validate(#[from] ValidateError),
    /// The requested SASL mechanism name was invalid.
    #[error("invalid SASL mechanism: {0}")]
    ValidateSaslMechanism(#[from] ValidateSaslMechanismError),
    /// A command atom was invalid.
    #[error("invalid atom: {0}")]
    ValidateAtom(#[from] ValidateAtomError),
    /// A `NOTIFY` command input was invalid.
    #[error("invalid notify settings: {0}")]
    ValidateNotify(#[from] ValidateNotifyError),
    /// Error appending an e-mail.
    #[error("could not append mail to mailbox")]
    Append,
}

/// An error occured while trying to parse a server response.
#[derive(thiserror::Error, Debug)]
pub enum ParseError {
    /// Indicates an error parsing the status response. Such as OK, NO, and BAD.
    #[error("unable to parse status response")]
    Invalid(Vec<u8>),
    /// An unexpected response was encountered.
    #[error("encountered unexpected parsed response: {0}")]
    Unexpected(String),
    /// The client could not find or decode the server's authentication challenge.
    #[error("unable to parse authentication response: {0} - {1:?}")]
    Authentication(String, Option<DecodeError>),
    /// The client received data that was not UTF-8 encoded.
    #[error("unable to parse data ({0:?}) as UTF-8 text: {1:?}")]
    DataNotUtf8(Vec<u8>, #[source] Utf8Error),
    /// The expected response for X was not found
    #[error("expected response not found for: {0}")]
    ExpectedResponseNotFound(String),
}

/// An [invalid character](https://tools.ietf.org/html/rfc3501#section-4.3) was found in an input
/// string.
#[derive(thiserror::Error, Debug)]
#[error("invalid character in input: '{0}'")]
pub struct ValidateError(pub char);

/// An invalid SASL mechanism name was passed to `AUTHENTICATE`.
#[derive(thiserror::Error, Debug, Clone, Eq, PartialEq)]
#[non_exhaustive]
pub enum ValidateSaslMechanismError {
    /// SASL mechanism names cannot be empty.
    #[error("mechanism must not be empty")]
    Empty,
    /// SASL mechanism names are limited to 20 ASCII bytes.
    #[error("mechanism must be at most 20 bytes, got {0}")]
    TooLong(usize),
    /// SASL mechanism names can only contain ASCII letters, digits, hyphen, and underscore.
    #[error("invalid character '{0}'")]
    InvalidChar(char),
}

/// An invalid IMAP atom was passed to a command.
#[derive(thiserror::Error, Debug, Clone, Eq, PartialEq)]
#[non_exhaustive]
pub enum ValidateAtomError {
    /// IMAP atoms cannot be empty.
    #[error("atom must not be empty")]
    Empty,
    /// Command atom lists cannot be empty.
    #[error("atom list must not be empty")]
    EmptyList,
    /// IMAP atoms cannot contain this character.
    #[error("invalid character '{0}'")]
    InvalidChar(char),
}

/// Invalid settings were passed to the RFC 5465 `NOTIFY` command.
#[derive(thiserror::Error, Debug, Clone, Eq, PartialEq)]
#[non_exhaustive]
pub enum ValidateNotifyError {
    /// `NOTIFY SET` needs at least one mailbox event group.
    #[error("notify settings must include at least one event group")]
    NoGroups,
    /// An event list was empty. Use `NotifyEvents::none()` to send `NONE`.
    #[error("notify event list must not be empty")]
    EmptyEventList,
    /// A `SUBTREE` or `MAILBOXES` filter had no mailbox names.
    #[error("notify mailbox list must not be empty")]
    EmptyMailboxList,
    /// `SELECTED` and `SELECTED-DELAYED` cannot both appear in one command.
    #[error("selected and selected-delayed cannot both be specified")]
    ConflictingSelectedModes,
    /// A selected mailbox filter appeared more than once.
    #[error("selected mailbox filter can only be specified once")]
    DuplicateSelectedFilter,
    /// `SELECTED` and `SELECTED-DELAYED` filters only allow message events.
    #[error("selected mailbox filters only allow message events")]
    SelectedOnlyMessageEvents,
    /// `MessageNew` and `MessageExpunge` must be requested together.
    #[error("MessageNew and MessageExpunge must be specified together")]
    MessageNewAndExpungeMustBeTogether,
    /// `FlagChange` requires `MessageNew` and `MessageExpunge`.
    #[error("FlagChange requires MessageNew and MessageExpunge")]
    FlagChangeRequiresMessagePair,
    /// `MessageNew` fetch attributes are only valid for selected mailbox filters.
    #[error("MessageNew fetch attributes are only valid for selected mailbox filters")]
    FetchAttributesOnlySelected,
    /// A `MessageNew` fetch attribute was empty.
    #[error("MessageNew fetch attributes must not be empty")]
    EmptyFetchAttribute,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn is_send<T: Send>(_t: T) {}

    #[test]
    fn test_send() {
        is_send::<Result<usize>>(Ok(3));
    }
}
