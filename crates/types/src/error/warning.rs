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

#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum WarningKind {
    StrategyDowngraded,
    OperatorAttentionNeeded,
    Throttled,
    ClockSkew,
    BlobNotByteStream,
    ReadbackSkipped,
    Other,
}
