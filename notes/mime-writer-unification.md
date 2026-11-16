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

## Link 1 - one structured header writer in bifrost-mime

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
