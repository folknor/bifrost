//! Error type for the shared SASL/SCRAM computation layer.

/// A SASL/SCRAM computation failure.
///
/// The crate is computation-only: these variants describe malformed protocol
/// messages or a failed SCRAM exchange, not transport or I/O errors. Each
/// protocol crate maps `SaslError` back into its own error enum at the call
/// boundary.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum SaslError {
    /// A malformed or unexpected SASL/SCRAM protocol message.
    #[error("SASL protocol error: {0}")]
    Protocol(String),
    /// The server reported an error in its SCRAM server-final message
    /// (the `e=` field), or server-signature verification failed.
    #[error("SCRAM authentication failed: {0}")]
    AuthFailed(String),
}
