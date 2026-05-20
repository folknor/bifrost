# Error model convergence across crates

bifrost-smtp landed a rich error model: `ErrorKind::Transient(Response)`
and `Permanent(Response)` carry the full server reply, `Timeout` is its
own variant classified at IO-error construction (not via source-chain
walking), and accessors `smtp_response()`, `enhanced_status_code()`,
and `is_smtp_reply()` expose reply detail to callers. bifrost-jmap and
bifrost-imap have their own error stories.

Open question: should they converge, at least conceptually? Convergence
buys ratatoskr a unified error-handling pattern (reply errors carry
full server context, timeouts are first-class, classification happens
once at construction). Divergence saves the refactor work and lets each
protocol model errors on its own terms.

Worth deciding before either crate does a non-trivial error refactor
of its own.
