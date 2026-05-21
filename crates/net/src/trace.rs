//! W3C `traceparent` header construction.
//!
//! The header format is
//! `00-<trace-id-32-hex>-<span-id-16-hex>-<flags-2-hex>` per W3C Trace
//! Context. Each outbound HTTP request emits a `traceparent` so
//! downstream services can correlate the request to its caller's
//! span.
//!
//! # Known gap: per-request trace_id
//!
//! True trace-context propagation requires reading the trace_id off
//! the current `tracing::Span`'s OpenTelemetry extension, which lives
//! in the `tracing-opentelemetry` crate. The bifrost workspace does
//! not depend on `opentelemetry` or `tracing-opentelemetry` today
//! (pulling them in for one header would add a fan-out of
//! TLS-adjacent transitive deps). As a result, the trace_id below is
//! freshly minted per HTTP request from a v4 UUID. That means N
//! requests issued inside one engine-level `bifrost.sync.changes`
//! span will appear as N independent traces to the receiver, instead
//! of N child spans of the engine's parent.
//!
//! The span_id half is best-effort accurate: when the request runs
//! inside an instrumented scope, we derive it from
//! `tracing::Span::current().id()` so the receiver sees the same
//! span_id across multiple requests of one operation. When there is
//! no current span we fall back to half a fresh UUID.
//!
//! When the workspace picks up `tracing-opentelemetry`, `trace_id_hex`
//! is the only function that needs to change; the public surface
//! (`current_traceparent`) is stable.

use uuid::Uuid;

/// Construct a `traceparent` header value for the current request.
///
/// The format is:
///
/// ```text
/// 00-<trace-id-32-hex>-<span-id-16-hex>-01
/// ```
///
/// `01` flags the trace as "sampled", which matches the convention
/// other bifrost-net consumers expect (downstream services will
/// emit child spans for sampled requests; we want them to do so).
#[must_use]
pub fn current_traceparent() -> String {
    let trace_id = trace_id_hex();
    let span_id = span_id_hex();
    // The version byte is `00` (only defined version). Flags `01`
    // marks the trace as sampled.
    format!("00-{trace_id}-{span_id}-01")
}

/// Mint a 32-hex-character trace identifier.
///
/// Currently a fresh v4 UUID per call. This breaks parent-trace
/// propagation: multiple requests under the same engine span receive
/// distinct trace_ids and cannot be reconnected receiver-side. See
/// the module docs for why this is acceptable today (no
/// `tracing-opentelemetry` dep) and what fix is queued.
fn trace_id_hex() -> String {
    let uuid = Uuid::new_v4();
    let bytes = uuid.as_bytes();
    let mut out = String::with_capacity(32);
    for b in bytes {
        out.push_str(&format!("{b:02x}"));
    }
    out
}

/// Mint a 16-hex-character span identifier. We pull from
/// `tracing::Span::current().id()`'s u64 when available (so multi-
/// request operations inside one span correlate), otherwise fall
/// back to half of a fresh UUID. The latter is what happens when a
/// request runs outside any `#[tracing::instrument]` scope.
fn span_id_hex() -> String {
    if let Some(id) = tracing::Span::current().id() {
        let raw = id.into_u64();
        return format!("{raw:016x}");
    }
    let uuid = Uuid::new_v4();
    let bytes = uuid.as_bytes();
    let mut out = String::with_capacity(16);
    for b in &bytes[..8] {
        out.push_str(&format!("{b:02x}"));
    }
    out
}
