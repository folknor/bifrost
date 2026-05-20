# Gmail followups

Tracker for known rough edges in `bifrost-gmail` after the initial vendoring
pass. None of these are blocking the crate from being usable, but each one
will eventually want a focused change.

Last verified: 2026-05-20.

## Typed error enum

Every public fallible function in the crate currently returns
`Result<_, String>`. See `crates/gmail/src/api.rs`, `client.rs`, `gdrive.rs`,
`headers.rs`, and `parse.rs`. This is the simplest possible error story and
makes the initial port painless, but it loses every distinction a caller
would actually want: HTTP status, transport vs decode failure, retryable vs
terminal, quota exhaustion, auth refresh needed, malformed payload.

Workspace convention is a typed `#[non_exhaustive]` enum per crate, matching
`bifrost-imap` and `bifrost-smtp`. The follow-up is to introduce a
`bifrost_gmail::Error` enum with variants for at least:

- transport (`reqwest::Error`)
- HTTP status with body excerpt
- JSON decode (`serde_json::Error`)
- base64url decode
- malformed Gmail payload (missing required field, unexpected shape)
- auth (token refused, refresh required)

Then thread it through the public surface and drop `Result<_, String>`
everywhere. Because the crate is pre-1.0 and `#[non_exhaustive]` is set, the
later addition of variants is not a breaking change for downstream matches.

## Folded headers in `inject_read_receipt_header`

`crates/gmail/src/headers.rs:17` walks the header block with `str::lines()`,
which treats each `\r\n`-delimited line independently. Per RFC 5322 a header
may be folded across multiple physical lines with continuation whitespace;
the current code would see `From:` on one line and the actual address on the
next without realising they belong together.

In practice Gmail-originated raw MIME is unfolded enough that this works for
the typical send path, which is why it was good enough to vendor. It is not
safe against:

- messages constructed by other clients and round-tripped through Gmail
- crafted MIME from tests or forensic fixtures
- any future caller that hands raw bytes from outside the Gmail API to this
  helper

The fix is to unfold the header block before scanning (collapse any CRLF
followed by SP or HTAB into a single space), in the same shape as
`auth_parser::normalize_header`. Worth pulling out a shared header
unfolding helper rather than copy-pasting the logic.

## Multiple `Authentication-Results` headers

`crates/gmail/src/auth_parser.rs:70` returns the first matching header by
case-insensitive name. RFC 8601 explicitly allows multiple
`Authentication-Results` headers, and in practice every relay hop in a
multi-hop delivery path adds its own. Gmail itself inserts one when the
message lands; mailing lists, forwarders, and corporate gateways add more.

Today we silently pick whichever the iterator finds first, which is
delivery-order dependent and not guaranteed to be the trust-anchor result
(usually the receiving MTA's own A-R, identified by `authserv-id`). For a
read-only verdict surfaced in the UI this can flip between hops in ways the
user cannot diagnose.

The fix needs two pieces:

- `find_header` should return all matches, not just the first.
- `parse_authentication_results` should select the correct one by
  `authserv-id`, defaulting to the receiver's own identity (configurable, or
  derived from the recipient domain). Fall back to the most recently added
  header (typically first in the list when headers are read top-down) if no
  identity is known.

ARC chains have the same shape and the same fix applies to
`ARC-Authentication-Results`.
