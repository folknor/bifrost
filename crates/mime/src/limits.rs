/// Caps that keep parsing hostile messages bounded.
#[derive(Debug, Clone, Copy)]
pub struct MimeLimits {
    pub max_input_bytes: usize,
    pub max_depth: usize,
    pub max_parts: usize,
    pub max_header_bytes: usize,
    pub max_text_bytes: usize,
}

impl Default for MimeLimits {
    fn default() -> Self {
        Self {
            max_input_bytes: 64 * 1024 * 1024,
            max_depth: 20,
            max_parts: 1000,
            max_header_bytes: 1024 * 1024,
            max_text_bytes: 4 * 1024 * 1024,
        }
    }
}

/// Advisory information about a lenient MIME parse.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Defect {
    Truncated,
    DepthExceeded,
    PartCountExceeded,
    HeaderBlockTooLarge,
    TextTruncated,
    MissingBoundary,
    UnknownTransferEncoding,
    MalformedBase64,
    MalformedQuotedPrintable,
    UnknownCharset,
    MissingHeaderSeparator,
}

#[cfg(test)]
#[path = "limits_tests.rs"]
mod tests;
