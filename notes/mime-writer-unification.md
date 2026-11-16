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

Link 2 is now built (see below). Link 3 (large) shares MIME entity
serialization.

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

### Link 2 partial implementation and hand-back (2026-11-16)

Built: deferred SMTP `message_id(None)` generation, resolved at build time,
with `message_id_domain(String)` as an explicit override. Single From wins over
Sender; multiple authors use their required Sender; envelope overrides never
select the author domain. Explicit Message-ID headers win. As before, without
a generation request the builder leaves Message-ID insertion to the relay.
The random 36-character local part is retained; hostname stays for EHLO.

Decision made: reject an unusable author domain rather than invent an identity.
Generated domains must be nonempty dot-atoms, with nonempty components, and at
most 947 octets (the Message-ID field prefix, 36 local octets, @ and angle
brackets must fit 998 even before shared folding is adopted). Domain literals require an explicit usable domain override.
An override is validated only when actually generating. This keeps a shared
mailbox's identity intact and gives an enterprise send queue an actionable
local failure rather than a machine-hostname identity or silently altered domain.

### Link 2 completed (2026-11-16)

The handed-back fallibility question was settled with codex (converged after
five rounds) and link 2 is built. The binding contract now lives in
`reference/smtp.md` ("Header encoding and its failure contract") and
`reference/mime.md`; in short:

- `Header::display` describes (name, raw value, grammar) and never encodes.
  Encoding happens once, when a value enters a `Headers`; `set` and
  `insert_raw` return `Result` and leave the old entry on refusal. So a
  `Headers` holds only validated bytes and formatting stays infallible.
  Rejected shapes: a fallible `display` (every chain breaks, even setters
  that cannot fail), lazy encoding at `formatted()` (`Display` cannot carry
  an error), and poisoned values carrying a deferred error.
- `MessageBuilder` latches the first refusal and its build step reports it
  first. Part builders end infallibly, so their `header` / `content_type`
  and `Attachment::body` return `Result`.
- The writer gained `Structured`, `Raw` (caller spelling, byte-for-byte
  when legal, which is what keeps a caller Date exact) and `PreEncoded`
  (framing only), and now encodes or refuses control octets everywhere.
- Content-Type is structured, not parameterized (RFC 2231 continuations on
  a 70-octet boundary break parsers). A boundary is added when a
  Content-Type enters a multipart builder (fallible); later edits splice the
  caller's spelling so every unit only shrinks and no parameter is lost.
  Kind parameters are bounded at 900 octets.
- Minted Date goes through `format_rfc5322_date`; `Date::new` is fallible
  (1900..=9999). Parsing still uses `httpdate`.
- DKIM signs the final bytes. Two real defects in the old model-based
  signer were confirmed and fixed: simple canonicalization hashed the
  configured name spelling (`h=from` against `From:`), and a multipart
  root `Content-Type` was never hashed because it lives in the part headers.
  The field is a fixed template with `b=` on its own line and a 72-octet
  folded signature; `sign` returns `Result` and refuses unverifiable
  configurations (non-DNS `d=`/`s=`, lookup name past 253, `;` in a signed
  name, From not signed).
- Message-ID: an IDN author domain is written as its A-label instead of
  being refused; the 947-octet bound gave way to the writer's refusal.

Validation: `brokkr check -p bifrost-mime -p bifrost-smtp` green, 2,461
tests across five sweeps; imap `compose` and google `send` tests pass
against the changed writer (their full scoped gate was red only on another
agent's in-flight imap file). Ablations bit: hashing the model headers or the
configured name spelling failed the independent final-byte verifier;
disabling the Raw passthrough failed the folded-Date test; ignoring the
builder latch failed the build-refusal test.

Leftover, filed here with sizes:

- Keywords and List-ID are stored unparsed and take the caller-spelling
  rule, so a single phrase word past 998 octets is refused where a phrase
  grammar could encode it. Typed phrase-list and List-ID grammars (small).
- `MultiPart::headers_mut()` can still replace Content-Type with one lacking
  a boundary, after which `boundary()` panics at format time; pre-existing,
  not introduced here (small).
- Link 3 below is unchanged (large).
