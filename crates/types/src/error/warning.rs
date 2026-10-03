use super::diagnostic::DiagnosticText;

#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct Warning {
    pub kind: WarningKind,
    pub message: DiagnosticText,
    pub next_action: Option<DiagnosticText>,
    pub protocol_detail: Option<DiagnosticText>,
    pub retry_count: u32,
}

impl Warning {
    #[must_use]
    pub fn new(kind: WarningKind, message: DiagnosticText) -> Self {
        Self {
            kind,
            message,
            next_action: None,
            protocol_detail: None,
            retry_count: 0,
        }
    }

    #[must_use]
    pub fn user_safe(kind: WarningKind, message: impl Into<String>) -> Self {
        Self::new(kind, DiagnosticText::user_safe(message))
    }

    #[must_use]
    pub fn support_only(kind: WarningKind, message: impl Into<String>) -> Self {
        Self::new(kind, DiagnosticText::support_only(message))
    }

    #[must_use]
    pub fn with_next_action(mut self, next_action: DiagnosticText) -> Self {
        self.next_action = Some(next_action);
        self
    }

    #[must_use]
    pub fn with_protocol_detail(mut self, protocol_detail: DiagnosticText) -> Self {
        self.protocol_detail = Some(protocol_detail);
        self
    }

    #[must_use]
    pub fn with_retry_count(mut self, retry_count: u32) -> Self {
        self.retry_count = retry_count;
        self
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum WarningKind {
    StrategyDowngraded,
    OperatorAttentionNeeded,
    Throttled,
    ClockSkew,
    BlobNotByteStream,
    ReadbackSkipped,
    /// The change-stream broadcast ring overflowed and batches were lost;
    /// the consumer must reconcile from its last durable checkpoint.
    ///
    /// Structural on purpose: a consumer's whole recovery path (detach,
    /// re-drive, reconcile) hangs on telling this apart from the other
    /// account-scoped warnings, and before this variant existed the only
    /// discriminator was the message-text prefix - a contract no one had
    /// signed. Minted exclusively by the multiplexer's `ChangesReceiver`
    /// when it converts `RecvError::Lagged` into an observable event.
    ChangeStreamLagged,
    /// An inventory walk ended without the `Done` or `Terminated` every walk
    /// must end in, so nothing proved how far the enumeration got, and the walk
    /// is not treated as complete: backfill withholds the scope's completion
    /// marker, and the inventory fusion walk establishes no cursor. Pages
    /// acknowledged earlier in the walk still stand. A producer defect, not
    /// something the user can fix, but the user sees a scope that never
    /// finishes and this says why. Minted by both engine inventory front ends.
    InventoryEndedUnannounced,
    Other,
}
