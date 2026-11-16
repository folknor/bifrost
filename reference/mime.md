# bifrost-mime reference

`bifrost-mime` is the workspace's RFC 5322 / MIME crate: the tolerant inbound
parser, header and transfer decoding, body selection, address-list parsing,
the submission-envelope resolver, and the outbound renderer the account
crates compose through.

## Placement and dependency direction

The crate depends on `bifrost-types`; nothing in `bifrost-types` depends on
it. The renderer turns a `compose::SendRequest` into wire bytes and reports
`AccountError`, and its address vocabulary is the contract's
`compose::Address`, so the contract types stay in the contract crate and
MIME sits above them. Crates that only need the account contract (JMAP,
Graph, the DAV crates, sync, net) do not compile MIME or its `base64` and
`encoding_rs` dependencies. Consumers are `bifrost-imap` (hydration,
BODYSTRUCTURE decoding, composition, raw and draft submission),
`bifrost-google` (hydration of the Gmail MIME tree, composition) and
`bifrost-smtp` (header-derived `Envelope::try_from` and the account-side
`Submission` facade). The dependency graph is acyclic:
imap -> smtp -> mime -> types.

Inbound parsing is deliberately tolerant (`parse_content_type` substitutes
text/plain on malformed input). It is never an outbound validator; outbound
declarations are not fed back through it.

## Address parsing

`parse_address_list` parses unfolded address-list values using the same
parser as `HeaderMap::addresses`: quoted commas and quoted-pairs are handled
before display-name RFC 2047 decoding. Group colons and list separators count
only outside quoted strings, nested comments, angle addresses and domain
literals. Quoted local parts retain spaces, tabs, colons and quoted-pairs
verbatim. This remains tolerant parsing, not complete RFC 5322 validation.
`parse_address_list_checked` and `HeaderMap::addresses_checked` return
`Result<Vec<Address>, Vec<String>>`: any rejected mailbox makes the whole
result an error containing the original rejected items, rather than a partial
recipient list. Empty lists and empty groups succeed. Unsupported syntax must
be treated as a rejection, not evidence that the mailbox is invalid.
The `parse_address_list` and `addresses` conveniences return only accepted
mailboxes. IMAP submission uses the checked forms through
`submission_envelope`; Google draft hydration also propagates rejected
addresses before rewriting. `HeaderMap::parse` accepts a standalone header
block, unfolding continuations and preserving repeated fields in wire order.
Its caller bounds the input; unlike full MIME parsing, it applies no
header-size limit itself.

## Header decoding bounds

Header decoding is bounded on two independent axes. `ENCODED_WORD_SCAN_LIMIT`
(998, the RFC 5322 line limit) caps what a SINGLE RFC 2047 candidate may scan,
so an overlong word is never recognized and is echoed verbatim.
`DECODED_OUTPUT_LIMIT` (64 KiB) caps the TOTAL decoded output of one
`decode_encoded_words` call, because chained in-window words in a legacy
single-byte charset expand about 2.2x with base64 contraction already applied.
Past the total cap the remaining words are emitted verbatim rather than
truncated - the RFC 2047 Section 6.3 display rule for a word the decoder will
not decode - so no bytes are lost and output stays under the cap plus the input
length.

## Message-ID extraction

`HeaderMap::message_ids` extracts nonempty structural angle-delimited
identifiers across all occurrences of a header, preserving order and ignoring
angle brackets inside quoted strings and nested comments. The free function
`first_message_id(raw)` borrows its result from the raw value, uses the same
scanner and retains a trimmed bare-value fallback when there is no structural
opening angle bracket. Empty or unfinished structural brackets do not become
bare identifiers.

## Submission envelope

`HeaderMap::submission_envelope` checks every From, Sender, To, Cc and Bcc
address and returns an optional reverse path plus ordered recipients. For one
author, From wins over Sender: an enterprise user sending on behalf of a
shared mailbox expects bounces at that mailbox rather than the delegate.
Without From, Sender wins; without either the caller chooses its default or
SMTP's null reverse path. Multiple authors require one Sender and use it as
the only unambiguous reverse path. Multiple Senders are refused. Unsupported
addresses and missing recipients are refused without dropping any address.
SMTP header conversion and IMAP raw/draft submission share this owner. The
recipient rule itself is `bifrost_types::compose::validate_send_recipients`.
`generate_message_id` owns fresh sender-domain correlation ids for both
composed messages and raw IMAP submissions missing a structural Message-ID.

## Renderer

`send_request_to_rfc5322` (send) and `render_rfc5322` over a borrowed
`ComposedMessage` (drafts, Google's `MailDocument`) emit a UTC `Date` first
header, RFC 2047 display names and Subject, text/html/`multipart/alternative`
bodies with a `multipart/mixed` wrapper for inline attachments, every part
base64, and a sender-domain `Message-ID` (never a machine hostname). The send
path strips `Bcc:` from the wire body and returns a separate Sent copy that
keeps it; a Bcc-only send carries `To: undisclosed-recipients:;`. Rendering
has no account identity and stamps `Protocol::Unknown`; account boundaries
stamp their protocol when propagating renderer failures.

`write_header` and `HeaderValue` own structured outbound header encoding.
The renderer uses address lists, message-id lists, unstructured text and
parameterized MIME values. Folding prefers 78 octets (76 for encoded words)
and occurs only at structural whitespace, never inside a quoted string or
angle address. Caller CR/LF is sanitized before writer-owned folds are added.
Long or non-ASCII phrases and long indivisible ASCII unstructured words use
RFC 2047; attachment parameters use UTF-8 RFC 2231 continuations when non-ASCII
or too long for a short quoted value. Small ASCII parameters remain escaped
quoted strings. The 998-octet hard limit includes the header name and folding
whitespace: an indivisible addr-spec, message-id or other structured token
that cannot fit fails as `Request(Malformed)`. Enterprise send queues must
report an unrepresentable value rather than submit a line a relay may truncate.
`format_address` remains a value-formatting convenience; `write_header` owns
physical-line enforcement.

`bifrost-smtp`'s `message` builder is a second, independent RFC 5322 writer
with its own header encoding, folding, Message-ID (hostname-based) and Date
policies; only envelope derivation is shared today (its
`Envelope::try_from(&Headers)` delegates to `submission_envelope`).

## File map

```
crates/mime/src/
  lib.rs        public surface
  charset.rs    legacy charset decoding (encoding_rs)
  header.rs     HeaderMap, address-list parsing, submission envelope, dates
  limits.rs     MimeLimits and Defect hostile-input bounds
  params.rs     Content-Type / Content-Disposition, RFC 2231 parameters
  parse.rs      tolerant MIME tree parser
  render.rs     outbound renderer, Date and Message-ID minting
  select.rs     body and attachment selection
  transfer.rs   base64 / quoted-printable transfer decoding
  words.rs      RFC 2047 encoded-word decoding
  writer.rs     structured outbound header encoding and folding
```
