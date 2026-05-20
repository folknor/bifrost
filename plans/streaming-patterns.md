# Streaming patterns across crates

The three crates handle streaming differently:

- IMAP: daaki-derived driver-task model with cancellation-safe
  streaming FETCH.
- JMAP: reqwest streams via the HTTP transport, `async-stream` for
  pushed events.
- SMTP: classic request-response over a synchronous or async-mirror
  connection.

Open question: are there common patterns worth pulling up?
Cancellation safety, backpressure, partial-result delivery, and
timeout propagation look similar across protocols if you squint, but
each crate has chosen its own primitives. A shared `bifrost-stream`
module might be premature, or it might be exactly the right time
before the third pattern (Graph, Gmail) lands and ossifies the
divergence. Cheap to defer, expensive to retrofit after a fourth
pattern shows up.
