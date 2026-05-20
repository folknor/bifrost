# API surface for ratatoskr

JMAP, IMAP, and SMTP have each diverged from their upstream shapes
(lettre and friends) toward bifrost's own conventions. Open question:
what does ratatoskr see at the boundary?

Three plausible shapes:

- Reach-into-each-crate (status quo). ratatoskr imports
  `bifrost_jmap::*`, `bifrost_imap::*`, `bifrost_smtp::*` directly and
  switches per protocol.
- A unified mailstore trait in a new bifrost crate. One `MailAccount`
  trait with per-backend implementations. Protects ratatoskr from
  per-protocol detail but forces lowest-common-denominator semantics.
- A ratatoskr-side adapter layer. ratatoskr owns the abstraction;
  bifrost stays per-protocol.

The third option keeps bifrost focused on protocol fidelity, which is
its stated job. Worth a quick sketch before any cross-cutting work
(auth, errors) is undertaken: those decisions look different if there
is a unified facade than if there is not.
