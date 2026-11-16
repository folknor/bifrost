# One RFC 5322 header and entity writer (design chain)

Residue of the MIME placement work. MIME now lives in `bifrost-mime` above
`bifrost-types` (see `reference/mime.md`). The workspace still has two
independent RFC 5322 writers: `bifrost-mime`'s renderer and `bifrost-smtp`'s
lettre-derived `message` builder. Envelope derivation is already shared
(`Envelope::try_from(&Headers)` delegates to
`HeaderMap::submission_envelope`); nothing else is.

This was sparred with codex during the STR-014 session. Agreed shape: the
shared code sits BELOW both composition interfaces. `SendRequest` composition
(`send_request_to_rfc5322`, `ComposedMessage`) and `MessageBuilder` keep their
own inputs and policies and call one structured header writer and, later, one
entity serializer. Routing the SMTP builder through `ComposedMessage` is
ruled out: it would remove builder capabilities (nested multiparts, custom
headers, explicit transfer encodings, pre-encoded bodies, multiple authors
with Sender, envelope overrides, retained Bcc). The builder is published and
stays; DKIM stays in bifrost-smtp.

## Defects that motivate link 1 (bifrost-mime renderer, read from code)

- `push_header` runs `sanitize_header`, which flattens the `CRLF SP` folds
  `encode_words` puts between the encoded words of a non-ASCII display name.
- To, Cc, Bcc, Reply-To, `References` and `In-Reply-To` are never folded, so a
  long recipient list or reference chain is one line past the RFC 5322
  998-octet limit. Only Subject folds. No test drives a long address list.
- Attachment `name=` / `filename=` carry raw UTF-8 for non-ASCII names; no
  RFC 2231 (smtp uses `email_encoding::headers::rfc2231`).

## Link 1 - one structured header writer in bifrost-mime (built)

- Typed header values (unstructured text, address list, msg-id list,
  parameterized value) with folding at structural boundaries (between
  addresses, between msg-ids, between encoded words), RFC 2047 for unstructured
  text and phrases, RFC 2231 for parameters.
- Hard-limit policy must be stated, not inherited: folding cannot repair an
  oversized indivisible token (a long addr-spec, a long msg-id, an unbreakable
  ASCII word in a structured field), and encoded words are not legal
  everywhere. Decide per header kind whether such a value is encoded (where
  legal), or the composition fails as `Request(Malformed)`. The smtp builder's
  current "preserve and let the line run long" rule (`HeaderValueEncoder`
  doc, notes/todo.md 998-octet entry) is the same question on the other side;
  answer both here.
- The bifrost-mime renderer adopts it first. The smtp builder still has its
  own writer after this link; that is accepted, the link does not claim
  unification.

### Link 1 implementation and rulings (2026-11-16)

Built: public `HeaderValue` / `write_header` in `bifrost-mime`, adopted by
`render_rfc5322` for addresses (including receipt targets), Subject, message-id
fields and attachment headers. RFC 2047 encoding now survives address folding;
recipient and reference chains fold; attachment parameters use RFC 2231.
`format_address` keeps its public convenience surface. The entity serializer
and SMTP writer remain separate, as planned.

Hard-limit ruling, binding on link 2 as well: prefer 78-octet lines (76 when
emitting encoded words), never exceed 998 including the name or continuation
whitespace. Encode long ASCII words in unstructured text and long/non-ASCII
phrases with RFC 2047, where it is legal. Short ASCII parameters stay escaped
quoted strings; long/non-ASCII parameters use UTF-8 RFC 2231 indexed encoded
continuations, in chunks of 15 source octets, with percent triplets kept whole.
Continuation decoding concatenates octets before UTF-8 decoding, so a scalar
may cross parameter segments. An oversized indivisible addr-spec, msg-id or
other structured token fails as `Request(Malformed)`; it is never encoded in a
position where encoded words are illegal. This rejects an unrepresentable
message locally instead of leaving an enterprise user's queued send vulnerable
to relay truncation or rejection. SMTP's old preserve-overlong-line policy is
superseded when link 2 adopts the writer.

Folding trusts no caller CR/LF, replacing them with spaces first. Structural
whitespace is preserved; quotes and angle addresses protect internal whitespace
from folding. Unstructured text uses its own whitespace boundaries, since
quotes and angle brackets there are ordinary text. Message-id inputs retain
existing bracketing policy: References/In-Reply-To carry caller syntax, while
Message-ID and Content-ID are wrapped by the renderer.

Validation: `brokkr check -p bifrost-mime -p bifrost-smtp` passed, 2,416 tests
across five sweeps. The five new hermetic tests cover renderer folding and
round trips, RFC 2231 parameter decoding, text encoding and hard-limit refusal.
Replacing the renderer with its pre-link implementation made all four
`writer_renderer` regressions fail in both MIME sweeps; the implementation was
restored afterward. No unscoped gate was run in the concurrently edited tree.

Remaining work: link 2 (medium/large) adopts the writer in SMTP, including any
adapter needed for its custom typed headers, implements author-domain ID
precedence and shared minted Date formatting, and pins final-byte DKIM signing.
Link 3 (large) shares MIME entity serialization. No decision handed back.

## Link 2 - smtp builder consumes the header writer and the policies

- `HeaderValueEncoder` / mailbox encoding delegate to the link 1 writer.
  bifrost-smtp already depends on bifrost-mime.
- Message-ID: today `message_id(None)` generates `<36 alnum@hostname>`
  immediately, possibly before `.from()`. Move generation to `build()`, from
  the resolved author domain, with an explicit-domain override. Precedence
  must be defined for multiple From with Sender, unusable domains, and an
  explicit envelope that differs from the headers (the envelope must not
  silently pick the author identity).
- Date: shared formatting for a builder-minted Date only. A caller-supplied
  raw Date header stays byte-for-byte (`raw_date_header_is_preserved`,
  `unparseable_raw_date_header_is_preserved`); never parse-and-rewrite.
- DKIM signs the final bytes the serializer emits; no folding or encoding pass
  may run after signing.
- `hostname` stays a dependency regardless (EHLO `ClientId` uses it).

## Link 3 - one MIME entity serializer

Body/part serialization (transfer-encoding choice, boundaries with collision
re-roll, CRLF normalization) shared between the two composers. The renderer's
"every part is base64, so boundary collision is impossible" comment on
`boundary()` must go once any part is not base64.

## Also open

- Two address-list parsers remain: bifrost-mime's tolerant
  `parse_address_list(_checked)` and smtp's nom `Mailbox`/`Mailboxes` FromStr
  (all-or-nothing, validates through `email_address` + IDNA). They have
  different contracts (tolerant inbound vs validated construction); merging
  them is a separate question from the writer.
