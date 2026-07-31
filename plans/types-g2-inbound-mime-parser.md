# types-G2: inbound MIME parser in bifrost-types

Technical implementation specification.

## Standing references (READ these before implementing or reviewing)

Every implementer and every reviewer of this spec MUST read the following
in full. They are the ground this work is built on and judged against.

- `reference/technical-implementation-spec.md` - the contract this
  document is written against.
- `reference/error-model.md` - the cross-cutting `AccountError` contract.
  Always required reading: every `Account` method returns
  `Result<_, AccountError>`, so any change is bound by it. This spec
  deliberately adds no new error kind; section "Error model impact"
  states why and that ruling is only defensible if the reader knows the
  contract.
- `TODO.md`, item **types-G2** (and the neighbouring **imap-T2** /
  **imap-T4** entries, which name overlapping code) - the source that
  spawned this spec.
- `reference/types.md` - bifrost-types module layout, the `Message` /
  hydration vocabulary, the not-`#[non_exhaustive]` construction rule.
- `reference/imap.md` - bifrost-imap account layer, hydration path,
  codec decode tree, the blob-support ruling this spec depends on.
- `reference/jmap.md`, `reference/graph.md`, `reference/google.md` -
  the three crates whose `Message` construction this spec migrates.

## The item

`bifrost-types::mime` today serializes OUTGOING RFC 5322 messages only.
There is no inbound parser, so no account crate has a shared way to turn
fetched octets into decoded text, HTML, and attachments.

The symptom is in bifrost-imap. `attrs_for_hydration` fetches
`BODY.PEEK[]` for `Full` / `FullWithBlobs`, and `fetch_to_message`
(`crates/imap/src/account/pim.rs`) assigns
`String::from_utf8_lossy(whole_wire_message)` to `Message::body_text`,
leaves `body_html` `None`, and leaves `attachments` empty. Multipart,
base64, and quoted-printable messages therefore surface wire source
instead of content. The defect is pinned by the existing IMAP test
`full_hydration_puts_the_whole_raw_message_in_body_text`.

## Survey of the ground

### What exists in bifrost-types

`crates/types/src/mime.rs`, 940 lines, all outbound: `RenderedMessage`,
`SubmissionEnvelope`, `ComposedMessage`, `send_request_to_rfc5322`,
`render_rfc5322`, `format_address`, RFC 2047 ENCODING (`encode_words`,
`encode_one_word`), header sanitizing, base64 wrapping, boundary and
Message-ID minting. `crates/types/src/lib.rs:33` declares `pub mod mime`
and line 82 re-exports from it. Dependencies already present: `base64`,
`bytes`.

`crates/types/src/hydration.rs` holds `Message` (not
`#[non_exhaustive]`, protocol impls construct it) with
`body_text: Option<String>`, `body_html: Option<String>`,
`attachments: Vec<BlobHandle>`, plus `date: Option<SystemTime>` and
`references: Vec<String>`.

`crates/types/src/blob.rs` holds `BlobHandle { id, size, content_type,
digest, capabilities }`. It carries NO filename, NO disposition, NO
content-id: it is an engine fetch descriptor, not an attachment
descriptor. This matters below.

### What already exists elsewhere and is the inbound half in disguise

Two bifrost-imap modules are protocol-neutral RFC decoders that happen to
live in the IMAP crate because IMAP needed them first:

- `crates/imap/src/codec/decode/encoded_words.rs` (244 lines):
  `decode_rfc2047(&[u8]) -> String`, the full RFC 2047 decoder including
  the section 6.2 inter-word whitespace rule, the section 6.3
  display-verbatim fallback, the `ENCODED_WORD_SCAN_LIMIT` linearity
  bound, Q-decoding, and `encoding_rs` charset conversion. Callers, with
  the exact import each uses (this matters for the deletion below):
  `codec/decode/envelope_fetch.rs:4` and `codec/decode/bodystructure.rs:3`
  both do `use super::encoded_words::decode_rfc2047;`, i.e. they name the
  MODULE, not the re-export; `types/rfc2231.rs:224` calls
  `crate::codec::decode::decode_rfc2047`; `codec/decode/mod.rs:44`
  declares `mod encoded_words;` and `:59` is the `pub(crate) use`
  re-export; and roughly 20 test cases in `codec/decode/tests.rs` ride
  that re-export via `use super::*`.
- `crates/imap/src/types/rfc2231.rs` (406 lines) plus its
  `rfc2231_tests.rs` (727 lines, attached by `#[path]`):
  `decode_rfc2231_params(&[(String, String)]) -> Vec<(String, String)>`,
  continuation reassembly, charset/language prefixes, percent decoding,
  the section 5 encoded-overrides-plain dedup. Sole non-test caller:
  `codec/decode/bodystructure.rs:282`.

`encoding_rs` is declared in `crates/imap/Cargo.toml:34` and is used by
exactly those two modules and nowhere else in the workspace.

Gap in what exists: `decode_rfc2231_params` takes ALREADY SPLIT
`(name, value)` pairs, because IMAP receives parameters pre-split by the
BODYSTRUCTURE grammar. An inbound parser reads a textual header value
(`multipart/mixed; boundary="x"; name*0="a"; name*1="b"`) and must
tokenize it first. That tokenizer does not exist anywhere and is a brick
of this spec.

`crates/smtp/src/message/{body,mimebody}.rs` mention quoted-printable but
on the OUTBOUND side (they encode). No inbound quoted-printable decoder
exists in the workspace.

### What the other account crates do (why this is IMAP-only wiring)

- JMAP (`crates/jmap/src/sync/pim.rs:2825-2893`): the server returns
  parsed `textBody` / `htmlBody` / `attachments`; `blob_handle_from_part`
  mints a `BlobHandle` per `EmailBodyPart`. No MIME parsing needed.
- Graph (`crates/graph/src/account/pim.rs:2404-2498`, and the EWS lane at
  1092-1151): Graph returns a parsed `body` object plus `$expand=attachments`.
  No MIME parsing needed.
- Google (`crates/google/src/account/pim.rs:890-928`, plus
  `account/blobs.rs:127` `blob_handles_for_message`): Gmail returns a
  parsed payload tree. No MIME parsing needed.
- CardDAV / CalDAV: no message hydration at all.

So the parser has exactly ONE production consumer today: bifrost-imap.
That does not make it an IMAP-local fix. The shared crate is where it
belongs because (a) the RFC 2047 and RFC 2231 decoders it must contain
are already being shared badly (they sit in bifrost-imap and are
protocol-neutral), (b) the outbound serializer already lives there and
the inbound parser is its mirror, and (c) any future raw-RFC5322 lane
(a `.eml` import, an SMTP-side reader, a JMAP server that only returns
`blobId`) gets it for free. The TODO says so explicitly: "Do not build
the parser as a side effect of an IMAP fix; it is a shared-crate design
item."

### The obstacle the survey exposes

`Message::attachments` is `Vec<BlobHandle>`. IMAP cannot mint a
`BlobHandle`: `reference/imap.md` and `crates/imap/src/account/mod.rs:384`
record the standing ruling that IMAP advertises
`BlobRangeSupport::No`, that `open_blob` / `open_blob_range` return
`Unsupported`, and that the private blob-id codec was DELETED. A handle
IMAP could hand out is a handle nobody can open.

Worse, `BlobHandle` carries no filename, no `Content-Disposition`, and no
`Content-ID`, so even JMAP/Graph/Google currently drop the attachment
metadata a UI needs to render an attachment row or resolve a `cid:` image
in an HTML body.

This is resolved inline, in "The attachment shape" below, by replacing
the field type. Per `reference/technical-implementation-spec.md` point 4,
the existing type earns no protection: pre-1.0 internal breakage is legal
and the alternative (IMAP inventing unopenable handles) is a lie in the
type system.

## Stopping rule / blast radius

IN scope:

1. A new `crates/types/src/mime/` module tree: the existing serializer
   moved verbatim into it, plus the inbound parser, the header model, the
   transfer decoders, the charset layer, the RFC 2047 and RFC 2231
   decoders moved out of bifrost-imap, and the body-selection rule.
2. `Message::attachments` retyped from `Vec<BlobHandle>` to
   `Vec<MessageAttachment>`, and `Message::date` / `Message::references`
   populated on the IMAP full-hydration path.
3. bifrost-imap: `encoded_words.rs` and `rfc2231.rs` deleted and their
   call sites repointed at bifrost-types; `fetch_to_message` rewritten to
   parse; `attrs_for_hydration` preview lane made parseable.
4. bifrost-jmap, bifrost-graph, bifrost-google: `Message` construction
   migrated to the new attachment shape (mechanical, wrapping their
   existing `BlobHandle` in the new source variant and filling the
   metadata they already hold).
5. Reference-doc updates for `types.md` and `imap.md`.

OUT of scope, and named so it is not mistaken for deferral:

- `HydratedObject` / `Projection` (the engine-side `get_stream` read
  path, `crates/types/src/mutation.rs:115`) keeps `Vec<BlobHandle>`. It
  is the binary engine shape, not the user-facing parsed one, and
  `reference/types.md` draws that line explicitly. Untouched.
- IMAP per-attachment byte streaming (`open_blob` on a
  `BODY[<part>]` section) stays `Unsupported`. That is the separate,
  already-closed TODO ruling in the imap section; this spec makes
  attachment CONTENT reachable at `FullWithBlobs` by shipping decoded
  bytes inline with the message, which is a different lane from a
  resumable blob handle.
- `HydrationProjection::Headers` continues to be served from IMAP
  `ENVELOPE` alone. No `BODY.PEEK[HEADER]` fetch is added.
- The outbound serializer's behavior. It moves file, byte for byte, and
  its tests move with it unchanged.
- bifrost-smtp. Its inbound surface is response text, not MIME.

## The target as concrete artifacts

### Module tree

```
crates/types/src/mime/
  mod.rs        module wiring + the public re-export surface
  limits.rs     MimeLimits + Defect (the foundation types every other
                module in the tree takes by reference; see brick 1)
  render.rs     the existing crates/types/src/mime.rs, moved verbatim
  words.rs      RFC 2047 decode (moved from bifrost-imap)
  charset.rs    charset label -> UTF-8
  params.rs     RFC 2231 param decode (moved) + the header-value tokenizer
  header.rs     header block model: split, unfold, typed accessors
  transfer.rs   base64 / quoted-printable / 7bit / 8bit / binary decode
  parse.rs      the entity parser: octets -> ParsedMessage
  select.rs     part tree -> DecodedBody (text / html / attachments)
  parse_tests.rs, select_tests.rs, header_tests.rs, transfer_tests.rs,
  params_tests.rs, words_tests.rs, limits_tests.rs
                                    (attached with #[path], matching the
                                     bifrost-imap convention)
```

`limits.rs` is not a stylistic split. `charset.rs` (brick 2) and
`header.rs` (brick 3) both take `&MimeLimits` and `&mut Vec<Defect>` in
their signatures, and both land before `parse.rs`. Defining those two
types in `parse.rs` would make bricks 2 and 3 red at their own
boundaries, which point 6 of the spec contract forbids. They live in
`limits.rs`, created by brick 1, and `parse.rs` re-exports nothing: the
public path is `mime::{MimeLimits, Defect}` through `mod.rs` either way.

`mod.rs` contains only:

```rust
mod charset;
mod header;
mod limits;
mod params;
mod parse;
mod render;
mod select;
mod transfer;
mod words;

pub use header::HeaderMap;
pub use limits::{Defect, MimeLimits};
pub use params::{
    ContentType, decode_rfc2231_params, parse_content_type, parse_disposition,
};
pub use parse::{
    MimePart, ParsedMessage, PartBody, TransferEncoding, parse_message,
    parse_message_with_limits,
};
pub use render::{
    ComposedMessage, RenderedMessage, SubmissionEnvelope, format_address, render_rfc5322,
    send_request_to_rfc5322,
};
pub use select::{DecodedAttachment, DecodedBody, SelectOptions, select_body, select_body_with};
pub use words::decode_encoded_words;
```

Three corrections against an earlier draft of this list, each of which
was a compile error rather than a preference:

- There is no `HeaderName` type. Header names are `&str` throughout
  (`HeaderMap::get(&str)`), so exporting `HeaderName` names nothing.
- `decode_rfc2231_params` MUST be exported: brick 2 instructs
  `codec/decode/bodystructure.rs` to import it from
  `bifrost_types::mime`, and `params` is a private module.
- `SelectOptions` MUST be exported: `select_body_with` is `pub` and takes
  it by value, so omitting it is a private-type-in-public-interface error
  AND leaves bifrost-imap unable to name the type it is told to
  construct.

`crates/types/src/lib.rs:33` (`pub mod mime;`) is unchanged. The line-82
`pub use mime::{...}` block gains `Defect`, `DecodedAttachment`,
`DecodedBody`, `MimeLimits`, `MimePart`, `ParsedMessage`, `PartBody`,
`SelectOptions`, `TransferEncoding`, `decode_encoded_words`,
`parse_message`, `select_body`, `select_body_with`.

### Types

`header.rs`:

```rust
/// One message-or-part header block, in wire order, unfolded.
///
/// Keys are stored twice: `name` verbatim (so a re-emit is faithful) and
/// a lowercase key for lookup. Values are the UNFOLDED raw value with
/// the leading space after the colon stripped and no RFC 2047 decoding
/// applied - decoding is a per-accessor decision, because a
/// `Content-Type` value must NOT be encoded-word decoded while a
/// `Subject` must.
///
/// Not `#[non_exhaustive]`: private fields, constructed only by the
/// parser.
#[derive(Debug, Clone, Default)]
pub struct HeaderMap {
    entries: Vec<HeaderEntry>,
}

#[derive(Debug, Clone)]
struct HeaderEntry {
    name: String,
    lower: String,
    value: String,
}

impl HeaderMap {
    /// First value for `name`, case-insensitive, raw and undecoded.
    pub fn get(&self, name: &str) -> Option<&str>;
    /// Every value for `name`, in wire order (Received, References).
    pub fn get_all(&self, name: &str) -> impl Iterator<Item = &str>;
    /// First value with RFC 2047 encoded-words decoded. For display
    /// headers only (Subject, Comments, Content-Description).
    pub fn get_decoded(&self, name: &str) -> Option<String>;
    /// Parse an address-list header (From, To, Cc, Bcc, Reply-To,
    /// Sender) into `compose::Address` values, display names
    /// encoded-word decoded.
    pub fn addresses(&self, name: &str) -> Vec<Address>;
    /// Parse an RFC 5322 date-time header into `SystemTime`.
    pub fn date(&self, name: &str) -> Option<SystemTime>;
    /// Split a msg-id-list header (References, In-Reply-To) into the
    /// individual ids WITHOUT angle brackets.
    pub fn message_ids(&self, name: &str) -> Vec<String>;
    pub fn iter(&self) -> impl Iterator<Item = (&str, &str)>;
    pub fn len(&self) -> usize;
    pub fn is_empty(&self) -> bool;
}
```

`params.rs`:

```rust
/// A parsed `Content-Type` value.
///
/// `ty` and `subtype` are lowercased; `params` keys are lowercased and
/// values are RFC 2231 decoded (continuations reassembled, charset and
/// language prefixes applied, percent-decoding done).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContentType {
    pub ty: String,
    pub subtype: String,
    pub params: Vec<(String, String)>,
}

impl ContentType {
    /// `text/plain`, allocated on demand.
    pub fn essence(&self) -> String;
    pub fn param(&self, name: &str) -> Option<&str>;
    pub fn charset(&self) -> Option<&str>;
    pub fn boundary(&self) -> Option<&str>;
    /// RFC 2045 section 5.2 default for a part with no Content-Type.
    pub fn text_plain_us_ascii() -> Self;
}

/// Tokenize and decode a `Content-Type` header VALUE.
///
/// Never fails: a value that does not parse yields
/// `ContentType::text_plain_us_ascii()` per RFC 2045 section 5.2
/// ("conformant" default on an unrecognized type).
pub fn parse_content_type(value: &str) -> ContentType;

/// Tokenize and decode a `Content-Disposition` header VALUE into the
/// disposition token (lowercased; `""` when absent) and its decoded
/// parameters.
pub fn parse_disposition(value: &str) -> (String, Vec<(String, String)>);

/// Moved from `crates/imap/src/types/rfc2231.rs`, visibility widened
/// from `pub(crate)` to `pub`. IMAP's BODYSTRUCTURE decoder keeps
/// calling this directly with pre-split pairs.
///
/// NOT moved verbatim: see "The RFC 2231 complexity fix" below.
pub fn decode_rfc2231_params(params: &[(String, String)]) -> Vec<(String, String)>;
```

#### The RFC 2231 complexity fix (mandatory, part of the move)

The current implementation is quadratic in the number of distinct
continuation parameter names, twice over:

- `crates/imap/src/types/rfc2231.rs:73` does
  `continuations.iter_mut().find(|(name, _, _)| *name == lower)` for
  EVERY continuation parameter, a linear scan of the group vector.
- `crates/imap/src/types/rfc2231.rs:165` calls
  `find_original_base_name(params, &lower_name)`, which rescans the ENTIRE
  input parameter list, once per group.

Today this is reachable only through BODYSTRUCTURE, which is why
`TODO.md` item **imap-T4** flags it as unresolved rather than urgent.
This spec adds a SECOND caller fed directly by an attacker-controlled
textual header value (the tokenizer in `parse_content_type`), so a header
of `n` distinct `a0*0=..;a1*0=..;..` parameters costs O(n^2) on the
inbound parse path. The move must therefore carry a rewrite:

- Replace the `Vec<(String, usize, BTreeMap<..>)>` group store with a
  `HashMap<String, usize>` from lowercased base name to group index,
  giving O(1) group lookup.
- Record the original-case base name at group CREATION time, from the key
  that created it. `find_original_base_name` and its rescan are deleted.
- Add an aggregate allocation cap: reject (drop, with a defect on the
  parse path and a `tracing::warn!` on the BODYSTRUCTURE path) once the
  summed reassembled byte length across all groups exceeds
  `MimeLimits::max_header_bytes`, so a continuation set cannot allocate
  more than the header block it came from.

This is the one place where "moved verbatim" does not apply, and the
existing `rfc2231_tests.rs` suite is the proof the rewrite is
behavior-preserving. It does not close imap-T4 (which also covers
BODYSTRUCTURE fuzzing), but it removes imap-T4's complexity concern from
both the old and the new caller.

The tokenizer (the piece that does not exist today) implements RFC 2045
section 5.1 `parameter` syntax: `;`-separated, `name=value`, value either
a `token` or a `quoted-string` with `\` escapes, whitespace around `=`
and `;` tolerated, an unterminated quoted-string consuming to end of
value, a `;` inside a quoted-string NOT splitting. It emits raw
`(name, value)` pairs and hands them to `decode_rfc2231_params`, so the
RFC 2231 semantics have exactly one implementation.

`transfer.rs`:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
pub enum TransferEncoding {
    #[default]
    SevenBit,
    EightBit,
    Binary,
    Base64,
    QuotedPrintable,
    /// A `Content-Transfer-Encoding` token this crate does not know
    /// (`uuencode`, `x-gzip64`). Body octets are passed through
    /// undecoded, a `Defect::UnknownTransferEncoding` is recorded, and
    /// the part's effective media type is FORCED to
    /// `application/octet-stream` per RFC 2045 section 6.4: "any entity
    /// with an unrecognized Content-Transfer-Encoding must be treated as
    /// if it has a Content-Type of application/octet-stream". Without
    /// that rule an `x-gzip64` part declaring `text/plain` would have its
    /// opaque octets selected as the rendered body.
    ///
    /// The forcing happens in `parse.rs` when it builds the `MimePart`:
    /// `MimePart::content_type` becomes
    /// `application/octet-stream`, so selection rule 8 routes the part to
    /// `attachments`. The ORIGINAL declared type is still recoverable
    /// from `MimePart::headers.get("Content-Type")`, which is what a
    /// faithful re-emit needs.
    Unknown,
}

impl TransferEncoding {
    /// Case-insensitive token lookup; unknown tokens map to `Unknown`.
    pub fn from_token(token: &str) -> Self;
}

/// Decode `body` per `encoding`. Never fails: base64 skips characters
/// outside the alphabet and tolerates missing padding; quoted-printable
/// emits a malformed `=XY` sequence literally. Both record a defect
/// through `defects` when they had to be lenient.
pub(super) fn decode_transfer(
    encoding: TransferEncoding,
    body: &[u8],
    defects: &mut Vec<Defect>,
) -> Vec<u8>;
```

`limits.rs`:

```rust
/// Caps that keep parsing a hostile message bounded.
///
/// Not `#[non_exhaustive]`: callers construct it to tighten a lane.
#[derive(Debug, Clone, Copy)]
pub struct MimeLimits {
    /// Octets of input parsed. Beyond this the input is truncated and
    /// `Defect::Truncated` is recorded. Default 64 MiB.
    pub max_input_bytes: usize,
    /// Maximum multipart nesting depth. A part deeper than this is kept
    /// as an undecoded leaf with `Defect::DepthExceeded`. Default 20.
    pub max_depth: usize,
    /// Maximum total parts in the tree. Default 1000.
    pub max_parts: usize,
    /// Octets of a single header block. Default 1 MiB.
    pub max_header_bytes: usize,
    /// Octets of DECODED text kept per text part. A charset expansion
    /// bomb (a 100 KB payload in a legacy multibyte charset) is cut here.
    /// Default 4 MiB.
    pub max_text_bytes: usize,
}

impl Default for MimeLimits { /* the values above */ }

/// Why a parse was lenient. Advisory: a mail client must render
/// something, so parsing never fails, it degrades and says how.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Defect {
    /// Input hit `max_input_bytes`, or a multipart ended without its
    /// closing `--boundary--` delimiter.
    Truncated,
    DepthExceeded,
    PartCountExceeded,
    HeaderBlockTooLarge,
    TextTruncated,
    /// `multipart/*` with no `boundary` parameter. The part is kept as a
    /// single leaf.
    MissingBoundary,
    UnknownTransferEncoding,
    MalformedBase64,
    MalformedQuotedPrintable,
    /// Charset label `encoding_rs` does not know, on the BODY path.
    /// Decoded as windows-1252.
    ///
    /// This is a deliberate COMPATIBILITY POLICY, not an RFC rule. RFC
    /// 2046 section 4.1.2 makes `us-ascii` the default charset and says
    /// nothing about unknown labels; the WHATWG Encoding Standard's
    /// replacement-and-windows-1252 behavior is a browser convention this
    /// crate borrows. It is chosen because unlabeled or mislabeled 8-bit
    /// mail is overwhelmingly single-byte western European, which
    /// windows-1252 renders and lossy UTF-8 turns into U+FFFD. Anyone
    /// revisiting this is revisiting a policy, not a conformance bug.
    UnknownCharset,
    /// No blank line separating headers from body. See `split_headers`
    /// for which side the input lands on: it depends on whether the
    /// first line looks like a header.
    MissingHeaderSeparator,
}

#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum PartBody {
    /// A leaf: transfer-decoded octets.
    Leaf(Vec<u8>),
    /// `multipart/*`: the child parts, in wire order. The preamble and
    /// epilogue are discarded (RFC 2046 section 5.1.1 says they are not
    /// content).
    Multipart(Vec<MimePart>),
    /// `message/rfc822`: the parsed embedded message AND the transfer-
    /// decoded octets it was parsed from.
    ///
    /// `raw` is not redundant. Selection rule 5 makes an embedded message
    /// an attachment whose bytes are "the message as received", and a
    /// re-serialization from `message` is lossy: headers have been
    /// unfolded, encoded-words decoded, and leaf bodies transfer-decoded,
    /// so the round trip would hand the consumer octets no server ever
    /// sent. Keeping the slice is one `Bytes` clone (refcount, not copy)
    /// against a guaranteed-wrong alternative.
    Embedded {
        raw: Bytes,
        message: Box<ParsedMessage>,
    },
}

/// One MIME entity.
///
/// Not `#[non_exhaustive]`: the parser is the only constructor but tests
/// and future consumers build one directly, and a new field must break
/// them so each answers the new question.
#[derive(Debug, Clone)]
pub struct MimePart {
    pub headers: HeaderMap,
    pub content_type: ContentType,
    pub encoding: TransferEncoding,
    /// Lowercased `Content-Disposition` token: `"inline"`,
    /// `"attachment"`, `""` when the header is absent.
    pub disposition: String,
    /// `filename` from Content-Disposition, else `name` from
    /// Content-Type, RFC 2231 decoded then RFC 2047 decoded (some agents
    /// emit encoded-words in a filename even though the RFC forbids it),
    /// then stripped of path separators and control characters.
    pub filename: Option<String>,
    /// `Content-ID` with the angle brackets removed.
    pub content_id: Option<String>,
    /// `Content-Description`, encoded-word decoded.
    pub description: Option<String>,
    pub body: PartBody,
}

impl MimePart {
    /// Decode a leaf body to a `String` using the part charset, capped at
    /// `MimeLimits::default().max_text_bytes`. `None` for a non-leaf.
    ///
    /// The cap is a parameter, not state: a `MimePart` deliberately does
    /// not carry the `MimeLimits` it was parsed under, because the limits
    /// that bound TREE construction (depth, part count, input size) are a
    /// different decision from the limit a given consumer wants on a
    /// given text decode, and charset decoding is lazy (the tree holds
    /// undecoded octets). `text()` is the convenience form; `text_with`
    /// is the one `select.rs` calls.
    pub fn text(&self) -> Option<String>;
    /// `text()` with an explicit cap and a defect sink, so
    /// `Defect::TextTruncated` and `Defect::UnknownCharset` reach a
    /// caller that has somewhere to put them.
    pub fn text_with(&self, limits: &MimeLimits, defects: &mut Vec<Defect>) -> Option<String>;
    /// Raw decoded octets of a leaf. `None` for a non-leaf.
    pub fn bytes(&self) -> Option<&[u8]>;
    pub fn is_multipart(&self) -> bool;
    /// The part is an attachment: disposition is `attachment`, or it
    /// carries a filename, or its type is neither `text/*` nor
    /// `multipart/*`.
    ///
    /// Deliberately does NOT try to except a `cid:`-referenced inline
    /// image. That question is not answerable from one part: it needs the
    /// SELECTED html body, which only `select.rs` knows. Selection rule 9
    /// owns it, and sets `DecodedAttachment::inline` accordingly. A
    /// `cid:` image is an attachment here and an inline attachment there;
    /// those are consistent, not contradictory.
    pub fn is_attachment(&self) -> bool;
}

/// A parsed message: the top-level header block plus the root entity.
/// The root's `headers` is the SAME block (MIME entity headers and
/// message headers share one block at the top level); duplicating rather
/// than aliasing keeps `MimePart` self-contained for recursion.
#[derive(Debug, Clone)]
pub struct ParsedMessage {
    pub headers: HeaderMap,
    pub root: MimePart,
    pub defects: Vec<Defect>,
}

/// Parse raw RFC 5322 octets with `MimeLimits::default()`.
///
/// Infallible by construction. See "Error model impact".
pub fn parse_message(raw: &[u8]) -> ParsedMessage;

pub fn parse_message_with_limits(raw: &[u8], limits: MimeLimits) -> ParsedMessage;
```

`select.rs`:

```rust
/// The flattened, consumer-facing view of a parsed message.
#[derive(Debug, Clone, Default)]
pub struct DecodedBody {
    pub text: Option<String>,
    pub html: Option<String>,
    pub attachments: Vec<DecodedAttachment>,
    /// Defects raised DURING selection (charset decode, text truncation),
    /// which happen after `parse_message` returned and so cannot be on
    /// `ParsedMessage::defects`. A consumer wanting the full picture
    /// concatenates the two.
    pub defects: Vec<Defect>,
}

#[derive(Debug, Clone)]
pub struct DecodedAttachment {
    pub filename: Option<String>,
    /// `type/subtype`, lowercased.
    pub content_type: String,
    pub content_id: Option<String>,
    /// `Content-Disposition: inline`, or absent-disposition with a
    /// `Content-ID` that the selected HTML body references as `cid:`.
    pub inline: bool,
    /// Decoded size in octets, always known even when `data` was
    /// dropped.
    ///
    /// Under `include_attachment_bytes: false` this must NOT be obtained
    /// by decoding and discarding. For `Base64` the decoded length is
    /// `3 * groups - padding` computed from the count of alphabet
    /// characters, no allocation; for `QuotedPrintable` it is the octet
    /// count minus soft breaks and minus 2 per `=XX`; for the identity
    /// encodings it is the slice length. Only `Unknown` falls back to the
    /// raw length. Decoding a 40 MB attachment to learn a number the
    /// caller explicitly said it does not want the bytes of is pure waste
    /// on the metadata lane.
    pub size: u64,
    /// Decoded octets, or `None` when the caller asked for metadata
    /// only.
    pub data: Option<Bytes>,
    /// The bytes are known to be incomplete: the parse hit
    /// `max_input_bytes`, or the enclosing multipart had no
    /// close-delimiter, so this attachment is a prefix.
    ///
    /// Exists so a consumer never presents a truncated payload as a whole
    /// file. See "Truncation must survive the hydration boundary".
    pub truncated: bool,
}

#[derive(Debug, Clone, Copy)]
pub struct SelectOptions {
    /// Populate `DecodedAttachment::data`. `false` yields metadata only.
    pub include_attachment_bytes: bool,
    /// Truncate `text` and `html` to this many BYTES on a char boundary.
    /// `None` for no truncation.
    ///
    /// This is the caller's ceiling; `MimeLimits::max_text_bytes` is the
    /// crate's. The effective cap passed to `MimePart::text_with` is the
    /// MINIMUM of the two, so a caller can tighten but never loosen the
    /// charset-expansion bound.
    pub max_body_bytes: Option<usize>,
    /// Limits for the per-part charset decode. Defaults to
    /// `MimeLimits::default()`; a caller that parsed with tightened
    /// limits should pass the same ones here.
    pub limits: MimeLimits,
}

pub fn select_body(message: &ParsedMessage) -> DecodedBody;
pub fn select_body_with(message: &ParsedMessage, options: SelectOptions) -> DecodedBody;
```

### The body selection rule (pinned, not left to the implementer)

Walk the tree depth-first. State: `text: Option<String>`, `html:
Option<String>`, `attachments: Vec<DecodedAttachment>`.

1. `multipart/alternative`: choose ONE representation per subtype by
   scanning children in REVERSE wire order (RFC 2046 section 5.1.4: the
   last alternative is the richest). The first `text/html` found in
   reverse order fills `html` if empty; the first `text/plain` fills
   `text` if empty. Children that are neither (a nested
   `multipart/related`, a `multipart/mixed`) are recursed into with the
   same rule. Alternatives NOT chosen contribute nothing, not even
   attachments: they are redundant renderings of the same content.
2. `multipart/related`: recurse into every child. The `start` parameter,
   if present and matching a child `Content-ID`, is visited first; all
   other children go to `attachments` (they are the `cid:` targets).
   Absent `start`, the first child is the root and the rest are
   attachments.
3. `multipart/mixed` and any unrecognized `multipart/*`: recurse into
   every child in wire order.
3a. `multipart/digest`: recurse into every child in wire order, but the
   DEFAULT `Content-Type` for a child with no `Content-Type` header is
   `message/rfc822`, not `text/plain; charset=us-ascii` (RFC 2046 section
   5.1.5). This is the one parent-sensitive default in MIME and the
   parser must carry it: `parse.rs` threads a `default_type:
   ContentType` argument down the multipart recursion, set to
   `message/rfc822` when the parent is `multipart/digest` and to
   `ContentType::text_plain_us_ascii()` everywhere else. Without it, a
   digest of bare forwarded messages selects the first forwarded
   message's headers as this message's body text.
4. `multipart/signed`: recurse into the FIRST child only (the content);
   the remaining children (the signature) become attachments. No
   verification, ever, in this crate.
4a. `multipart/encrypted`: NO child becomes the body. RFC 1847 section
   2.2 makes the first child the CONTROL INFORMATION (for
   `application/pgp-encrypted`, the literal bytes `Version: 1`) and the
   second child the ciphertext. Treating the first child as content, as
   the `multipart/signed` rule would, puts a protocol version string in
   `body_text`. Both children become attachments, `text` and `html` stay
   `None`, and the consumer sees an encrypted message it must hand to
   something that can decrypt. No decryption, ever, in this crate.
5. `message/rfc822` leaf: becomes an attachment with `content_type`
   `message/rfc822` and, when bytes are requested, `PartBody::Embedded`'s
   `raw` field: the transfer-decoded octets AS RECEIVED, not a
   re-serialization of the parsed form. It is not recursed into for body
   selection: an embedded forwarded message is not this message's body.
6. `text/plain` leaf: if `is_attachment()` (it has a filename or an
   `attachment` disposition) it goes to `attachments`; otherwise it fills
   `text` if empty, and goes to `attachments` if `text` is already set.
7. `text/html` leaf: same rule against `html`.
8. Any other leaf: `attachments`.
9. After the walk, for every attachment with a `content_id` and no
   explicit disposition, set `inline = true` if and only if the selected
   `html` contains `cid:<that id>` (case-insensitive on the scheme, exact
   on the id).

A message with no `MIME-Version` header and no `Content-Type` is a
single `text/plain; charset=us-ascii` leaf, which rule 6 turns into
`text`. That is the RFC 2045 section 5.2 default and it is the common
case for old and machine-generated mail.

### The attachment shape (the obstacle, resolved)

`crates/types/src/hydration.rs` changes:

```rust
/// Where an attachment's bytes come from.
///
/// Two lanes because two protocol shapes exist and collapsing them lies
/// in one direction or the other. A provider with a server-side blob
/// endpoint (JMAP `blobId`, Gmail `attachmentId`, Graph attachment id)
/// hands back a `BlobHandle` the consumer opens on demand, so a 40 MB
/// attachment is never carried in a hydration response. A provider that
/// only ever ships whole RFC 5322 octets (IMAP) has NO openable handle:
/// `reference/imap.md` records that IMAP advertises
/// `BlobRangeSupport::No` and that `open_blob` returns `Unsupported`, so
/// minting a handle there would produce a token nothing can redeem. IMAP
/// therefore ships the decoded octets it already holds, inline.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum AttachmentSource {
    /// Fetch on demand through `Account::open_blob`.
    Blob(BlobHandle),
    /// Bytes already decoded and carried with the message. Present only
    /// under `HydrationProjection::FullWithBlobs`.
    Inline(Bytes),
    /// Metadata only: the projection did not ask for bytes and the
    /// protocol has no handle to offer.
    None,
}

/// One attachment on a hydrated message.
///
/// Not `#[non_exhaustive]`: protocol Account impls construct it, and a
/// new field must break every constructor so each one answers the new
/// question rather than silently defaulting it. Same rule as `Message`
/// and `Page` in `reference/types.md`.
#[derive(Debug, Clone)]
pub struct MessageAttachment {
    pub filename: Option<String>,
    /// `type/subtype`, lowercased. `None` when the protocol did not say.
    pub content_type: Option<String>,
    /// `Content-ID` without angle brackets. An HTML body referencing
    /// `cid:<id>` resolves to this attachment.
    pub content_id: Option<String>,
    /// Render inside the body rather than as an attachment row.
    pub inline: bool,
    pub size: Option<u64>,
    pub source: AttachmentSource,
    /// The bytes behind `source` are known to be a PREFIX, because the
    /// parse that produced them was truncated. Always `false` for
    /// `AttachmentSource::Blob` (a handle is fetched whole on demand).
    pub truncated: bool,
}
```

and `Message::attachments` becomes `Vec<MessageAttachment>`.

#### Truncation must survive the hydration boundary

`Message` also gains:

```rust
    /// The body and attachments on this message are known to be
    /// incomplete: the source octets were truncated, or a limit cut a
    /// text part, or a multipart never closed.
    ///
    /// Set by any protocol impl that parses. `false` for the providers
    /// that receive already-parsed bodies.
    pub incomplete: bool,
```

Without this the defect model has a hole exactly where it matters. The
default limits truncate input at 64 MiB and text at 4 MiB;
`fetch_to_message` maps decoded fields onto `Message` and drops
`ParsedMessage::defects` on the floor; and "Error model impact" below
rules, correctly, that a degraded parse is not an `AccountError`. The
consequence, unfixed, is that `FullWithBlobs` can hand back a truncated
attachment as an ordinary `AttachmentSource::Inline` with no signal at
all - a silently corrupt file, presented as a whole one. That is worse
than either an error or a missing attachment.

`incomplete` is the minimum honest signal and it is deliberately a flag,
not a projected `Vec<Defect>`: `Defect` is a parser-shaped diagnosis with
variants (`DepthExceeded`, `MalformedQuotedPrintable`) that mean nothing
to a consumer choosing what to render, and exporting it onto `Message`
would make every future parser-internal variant a breaking change to the
hydration surface. The full defect list stays reachable on
`ParsedMessage` for any consumer that parses directly.

`fetch_to_message` sets `incomplete` when `ParsedMessage::defects` or
`DecodedBody::defects` contains any of `Truncated`, `TextTruncated`,
`DepthExceeded`, `PartCountExceeded`, or `HeaderBlockTooLarge`. The
lenient-decode defects (`MalformedBase64`, `UnknownCharset`,
`UnknownTransferEncoding`, `MissingBoundary`, `MissingHeaderSeparator`)
do NOT set it: those produce complete output from malformed input, which
is a different thing from partial output.
`crates/types/src/lib.rs` line 138 re-export becomes
`pub use hydration::{AttachmentSource, HydrationProjection, Importance, Message,
MessageAttachment, ThreadHydration};`.

`crates/types/src/mutation.rs:115` (`HydratedObject::blobs:
Vec<BlobHandle>`) is NOT touched, per the stopping rule.

### bifrost-imap after the change

`attrs_for_hydration` (`crates/imap/src/account/pim.rs:1960`):

```rust
/// Octets fetched for a `Preview` projection.
///
/// A preview cannot be taken from `BODY[TEXT]<0.n>`: for a multipart
/// message those first n octets are the boundary and the part headers,
/// not text, which is the whole defect this replaces. So the preview
/// fetches a bounded PREFIX of the whole message and parses it. The
/// parser tolerates a truncated tail (an unterminated part becomes a
/// leaf carrying `Defect::Truncated`), so a prefix is enough to produce
/// the first body part of a conventionally ordered message.
///
/// This is a HEURISTIC FLOOR, not a guarantee, and the spec says so
/// rather than pretending otherwise. A message whose header block runs
/// to the permitted 1 MiB, or whose first megabyte is an inline image,
/// yields an empty preview. That is an acceptable preview outcome (the
/// consumer shows no snippet) and an unacceptable body outcome, which is
/// why only the preview lane uses it. 64 KiB covers the header block
/// plus the first text part of the overwhelming majority of mail while
/// bounding what a preview is allowed to cost.
const PREVIEW_FETCH_BYTES: u64 = 64 * 1024;
```

- `Headers`: unchanged (`Uid`, `Flags`, `Envelope`, `Rfc822Size`).
- `Preview(limit)`: `BodySection { peek: true, section: None, partial:
  Some((0, max(limit as u64, PREVIEW_FETCH_BYTES))) }`. The
  `section: Some("TEXT")` fetch is deleted.

  The `max` is load-bearing. `HydrationProjection::Preview(usize)` is
  documented as "Headers + N bytes of preview text", and the current code
  passes N straight through as the partial length. Fetching a flat 64 KiB
  and then applying N only as `max_body_bytes` post-parse would silently
  under-serve every caller asking for more than 64 KiB: the consumer
  requests 256 KiB of preview and receives at most 64 KiB of source,
  minus headers and MIME framing. The floor exists to make a SMALL N
  usable (N bytes of raw multipart source is boundary markers, which is
  the defect being fixed); it must not become a ceiling on a large one.
- `Full` / `FullWithBlobs`: unchanged (`BodySection { peek: true, section:
  None, partial: None }`).

The `_ => {}` arm at line 1981 is RETAINED, with a comment naming the
reason. It is not dead code and it cannot be deleted:
`HydrationProjection` is `#[non_exhaustive]`, and `#[non_exhaustive]` is
scoped per CRATE, not per workspace. The enum is defined in bifrost-types
and matched in bifrost-imap, so rustc requires a wildcard here today and
after this change, regardless of the four arms being exhaustive in
practice. (Contrast a match on the same enum inside `crates/types`, where
the attribute has no effect and the wildcard would genuinely be
unreachable.) An earlier draft of this spec instructed deleting it; that
instruction was a guaranteed compile error.

`fetch_to_message` gains, in place of the `String::from_utf8_lossy` line:

```rust
let parsed = raw_body.map(|bytes| bifrost_types::mime::parse_message(&bytes));
let decoded = parsed.as_ref().map(|message| {
    bifrost_types::mime::select_body_with(
        message,
        SelectOptions {
            include_attachment_bytes: matches!(
                projection,
                HydrationProjection::FullWithBlobs
            ),
            max_body_bytes: match projection {
                HydrationProjection::Preview(limit) => Some(limit),
                _ => None,
            },
            limits: MimeLimits::default(),
        },
    )
});
```

and the `Message` fields become:

- `body_text` / `body_html`: from `decoded`, `None` under `Headers`.
- `attachments`: `decoded.attachments` mapped to `MessageAttachment` with
  `source: AttachmentSource::Inline(bytes)` when bytes were requested and
  `AttachmentSource::None` otherwise, `truncated` carried across. Empty
  under `Headers` and `Preview`. Metadata (no bytes) IS reported under
  `Full`; see "One cross-provider projection rule" below.
- `incomplete`: true when the parse or the selection recorded a
  truncation-class defect, per "Truncation must survive the hydration
  boundary".
- `date`: `parsed.headers.date("Date")` when parsed, else `None`. (The
  IMAP `ENVELOPE` date is a string this crate does not currently parse;
  the header lane is the one that lands here. `Headers` projection keeps
  `None`, unchanged behavior.)
- `references`: `parsed.headers.message_ids("References")` when parsed,
  else `Vec::new()`.
- `subject` / addresses / `in_reply_to`: unchanged, still from
  `ENVELOPE`, which the IMAP codec already RFC 2047 decodes. The parsed
  header block is NOT consulted for them: two sources for one field is
  how they drift.

Deletions in bifrost-imap:

- `crates/imap/src/codec/decode/encoded_words.rs` deleted, and with it
  `codec/decode/mod.rs:44` (`mod encoded_words;`), which would otherwise
  name a file that no longer exists.
  `codec/decode/mod.rs:59` becomes
  `pub(crate) use bifrost_types::mime::decode_encoded_words as decode_rfc2047;`

  That re-export keeps the ~20 cases in `codec/decode/tests.rs`
  compiling, because they reach the name through `use super::*`. It does
  NOT keep `envelope_fetch.rs` and `bodystructure.rs` compiling: both
  import through the module path (`use super::encoded_words::decode_rfc2047;`
  at `envelope_fetch.rs:4` and `bodystructure.rs:3`), and that module is
  gone. Both lines change to `use super::decode_rfc2047;`. An earlier
  draft claimed these two files were untouched; they are not, and the
  brick-2 gate list is written assuming they were edited. The two helpers
  `encoded_word_window` and `decode_q_encoding`, currently `pub(super)`
  and used only inside `encoded_words.rs` and its sibling tests, move
  with it and become private to `words.rs`; any test in
  `codec/decode/tests.rs` that calls them directly moves to
  `crates/types/src/mime/words_tests.rs`.
- `crates/imap/src/types/rfc2231.rs` and `rfc2231_tests.rs` deleted and
  moved to `crates/types/src/mime/params.rs` and `params_tests.rs`.
  `crates/imap/src/types/mod.rs:25` (`pub(crate) mod rfc2231;`) deleted.
  `codec/decode/bodystructure.rs:9` becomes
  `use bifrost_types::mime::decode_rfc2231_params;`.
- `crates/imap/Cargo.toml:34` `encoding_rs` dependency deleted (nothing
  else in the crate uses it).

`crates/imap/src/account/capabilities.rs:44` and
`crates/imap/src/account/mod.rs:384-392` keep their `Unsupported` blob
lane and their comments, with the comment extended to say that decoded
attachment BYTES now reach the consumer inline under `FullWithBlobs`
while a resumable per-part HANDLE remains unsupported.

### The other three crates

Mechanical migration, no behavior change:

- JMAP `crates/jmap/src/sync/pim.rs`: `blob_handle_from_part` (line 2920)
  becomes `attachment_from_part(part: &EmailBodyPart) ->
  Option<MessageAttachment>`, filling `filename` from
  `part.name()`, `content_id` from `part.content_id()`, `inline` from
  `part.content_disposition() == Some("inline")`, `size` from
  `part.size()`, `content_type` from `part.content_type()`, and
  `source: AttachmentSource::Blob(handle)` with the same handle it builds
  today. `rebase_foreign` (line 1240) takes
  `&mut [MessageAttachment]` and rewrites the id only for the
  `Blob` variant. The test at line 2976 is updated to the new shape.

  This migration is NOT purely mechanical, and calling it so would ship a
  regression. Two corrections:

  - The getters are `content_id()` (`crates/jmap/src/email/get.rs:216`)
    and `content_disposition()` (`:212`). There are no `cid()` or
    `disposition()` methods; an earlier draft named both.
  - Fixing the names is not enough, because the REQUEST does not ask for
    the data. `crates/jmap/src/sync/pim.rs:2804-2808` requests exactly
    `PartId`, `BlobId`, `Size`, `Name`, `Type`. `BodyProperty::Cid` and
    `BodyProperty::Disposition` both exist
    (`crates/jmap/src/email/mod.rs:720-721`) and must be ADDED to that
    list, or the corrected getters return `None` on every attachment and
    JMAP silently loses `cid:` resolution and the inline flag - the exact
    metadata gap this spec exists to close.
- Google `crates/google/src/account/blobs.rs:127`
  `blob_handles_for_message` becomes `attachments_for_message` returning
  `Vec<MessageAttachment>`, filling filename / content-id / inline from
  the `GmailPayload` part headers it already walks, and wrapping the
  existing handle in `AttachmentSource::Blob`. `open_blob` still receives
  a `BlobHandle`, so `account/mod.rs:328` is untouched; the call site in
  `account/pim.rs:901` takes the new type.
- Graph `crates/graph/src/account/blob.rs` keeps minting `BlobHandle`
  (its `open_blob` lane is live); `account/pim.rs:1113` and `:2449` wrap
  each handle into a `MessageAttachment`, filling `filename` from the
  Graph `name` field, `content_id` from `contentId`, and `inline` from
  `isInline`, all of which the expanded attachment JSON already carries.
  The EWS lane at `:1092-1151` does the same from its own item shape.

#### One cross-provider projection rule

The providers disagree today about when attachments appear at all, and
the retype is the moment to settle it rather than inherit the drift:

- JMAP builds attachments under `Full | FullWithBlobs`
  (`crates/jmap/src/sync/pim.rs:2838`).
- Google builds them only under `FullWithBlobs`
  (`crates/google/src/account/pim.rs:901`).
- Graph builds them only under `FullWithBlobs`
  (`crates/graph/src/account/pim.rs:2413`).

So a UI asking for `Full` sees an attachment list from one account and an
empty one from another, for messages that both have attachments. The rule
this spec pins, and which all four crates implement:

- `Headers`, `Preview(_)`: `attachments` is empty.
- `Full`: attachment METADATA is populated (filename, content type,
  content id, inline, size), with no bytes. `source` is
  `AttachmentSource::Blob(handle)` where the protocol has an openable
  handle (JMAP, Google, Graph) and `AttachmentSource::None` where it does
  not (IMAP).
- `FullWithBlobs`: as `Full`, except IMAP upgrades `None` to
  `Inline(bytes)`. The three handle-bearing providers keep `Blob`: they
  already have a cheap on-demand fetch, and inlining a 40 MB attachment
  into a hydration response to satisfy a naming symmetry would be a
  performance regression, which is precisely the asymmetry
  `AttachmentSource` exists to express.

That makes `Full` versus `FullWithBlobs` mean one thing everywhere: "do
you want the octets carried with the message", not "do you want to know
there are attachments". Google's `pim.rs:901` and Graph's `pim.rs:2413`
gates therefore widen to `Full | FullWithBlobs`.

`crates/imap/src/account/test_support.rs`, `crates/caldav/src/account.rs`,
`crates/carddav/src/account.rs`, and the four `crates/sync/tests/*.rs`
stub accounts reference `BlobHandle` only in their `open_blob` signatures,
which do not change. No edit needed there unless one constructs a
`Message` with attachments; a workspace build after brick 5 confirms.

## Error model impact

READ `reference/error-model.md` before judging this section.

`parse_message` returns no `Result` and mints no `AccountError`. That is
deliberate and it is the correct reading of the error model, not an
evasion of it:

- `AccountError` classifies a failed OPERATION into a `RecoveryClass`, so
  the engine can decide to retry, back off, reauthenticate, or surface.
  A malformed message body is none of those: retrying the fetch returns
  the same octets, and no credential or backoff changes the outcome.
- The operation SUCCEEDED. The server delivered the bytes it holds. A
  parse that degrades has not failed the `message_hydrate` call, and
  returning `Err` would deny the consumer a message it could partly
  render, which is exactly the failure mode of a mail client nobody wants.
- The `Defect` list carries the diagnosis where it belongs: on the datum,
  not on the operation.

Consequently no new `AccountErrorKind`, no new `Cause` variant, no new
message-key namespace entry, and no change to the `RecoveryClass` derive
mapping. `fetch_to_message` remains infallible in its body-decoding half
and keeps returning `Option<Message>` for the `uid`-missing case only.

The one place a defect could reasonably surface upward is a
`SyncEvent::Warning` on a hydration stream. `message_hydrate` is
request-response, not a stream, so there is no warning lane to put it in,
and inventing one is a separate design item that this spec does not open.
The full `Defect` list is reachable on `ParsedMessage` and `DecodedBody`
for any consumer that parses directly.

That is not, however, a licence to drop the information entirely at the
hydration boundary. "This message is fine but here is a diagnosis" and
"this message is a fragment and I am not telling you" are different
outcomes, and only the first is what this section argues for. The
truncation subset is projected onto `Message::incomplete` and
`MessageAttachment::truncated`; see "Truncation must survive the
hydration boundary" above. Those are data on the datum, exactly as this
section requires, not an error on the operation.

## Implementation order

Six landings. Each is one coherent, fully intrusive change; each leaves
`brokkr check` green at its boundary; each is kept or reverted on its
gates. No feature flags, no env-var switches, no gated probes.

### Brick 1: create the module tree, move the serializer, land the foundation types

Convert `crates/types/src/mime.rs` into `crates/types/src/mime/mod.rs` +
`crates/types/src/mime/render.rs`. `render.rs` receives the current file
contents VERBATIM including its `#[cfg(test)] mod tests`. `mod.rs`
contains `mod render;` and `pub use render::{...}` reproducing the exact
names `lib.rs:82` re-exports today. Add `encoding_rs = "0.8"` to the
workspace `[workspace.dependencies]` table in the root `Cargo.toml`
(alongside the existing `base64 = "0.23.0"` entry at line 48) and
`encoding_rs = { workspace = true }` plus `chrono = { workspace = true }`
to `crates/types/Cargo.toml`.

ALSO add `crates/types/src/mime/limits.rs` with `MimeLimits` (including
its `Default` impl) and `Defect`, exported from `mod.rs`. Nothing
consumes them yet. They land here because bricks 2 and 3 both take them
in signatures and neither can compile without them, and this spec's own
contract requires every brick to be green at its boundary. Landing two
plain data types one brick early is cheaper than merging bricks 2
through 4 into one unreviewable landing.

Why first: the rest is a pure move, so any breakage it causes is import
breakage the compiler names, and every later brick lands inside a tree
that already exists.

On `chrono`: brick 3's date parser needs civil-date-to-`SystemTime`
arithmetic (leap years, month lengths, the epoch conversion) plus
obsolete-zone handling. bifrost-types currently depends on `base64`,
`futures`, `bytes`, `thiserror`, `tracing`, `serde` and has no date
library, so declining the dependency means hand-writing that arithmetic -
the single largest unpriced item in this spec, and a class of code with a
long history of off-by-one bugs at month and year boundaries. `chrono` is
already in the workspace table (root `Cargo.toml:56`) and already a
bifrost-imap dependency (`crates/imap/Cargo.toml:33`), so this adds no
new third-party code to the build graph. It is taken.

Gates:

- `brokkr test -p bifrost-types send_request_builds_multipart_mixed_with_alternative`
- `brokkr test -p bifrost-types encoded_word_respects_75_octet_cap`
- `brokkr test -p bifrost-types mime_limits_defaults_are_the_documented_values`
- `brokkr check`

The two named cases are the strongest existing assertions in the moved
file (structure and RFC 2047 encoding); if the move dropped or mangled
content they fail. `brokkr check` covers the re-export surface across
every consuming crate.

### Brick 2: move RFC 2047 and RFC 2231 out of bifrost-imap

Move `encoded_words.rs` to `crates/types/src/mime/words.rs`, renaming the
public entry point `decode_rfc2047` to `decode_encoded_words` (the crate
it now lives in is not IMAP and the RFC number in the name buys nothing
that the doc comment does not) and widening it to `pub`.
`encoded_word_window`, `decode_q_encoding`, `hex_digit`,
`parse_encoded_word`, `parse_encoded_word_inner`,
`ENCODED_WORD_SCAN_LIMIT` all move and become private to the module.
Extract the charset branch at the tail of `parse_encoded_word_inner`
(lines 144-157) into `charset.rs`:

```rust
/// Decode `bytes` labeled `charset` into UTF-8.
///
/// UTF-8 / US-ASCII / ASCII short-circuit through
/// `String::from_utf8_lossy`. Everything else goes through
/// `encoding_rs::Encoding::for_label` with
/// `decode_without_bom_handling`, which preserves a leading U+FEFF that
/// is genuinely content rather than a BOM (header fragments and body
/// parts are not standalone documents).
///
/// An UNKNOWN label falls back to windows-1252 and records
/// `Defect::UnknownCharset`. Lossy UTF-8 would be the wrong fallback:
/// unlabeled or mislabeled 8-bit mail is overwhelmingly a single-byte
/// western European encoding, and windows-1252 renders it, where lossy
/// UTF-8 renders U+FFFD.
pub(super) fn decode_charset(charset: &str, bytes: &[u8], defects: &mut Vec<Defect>) -> String;

/// The same decision without a defect sink, for the RFC 2047 path, which
/// predates the defect model and whose callers (IMAP envelope decoding)
/// have nowhere to put one. Returns `None` for an unknown label so the
/// encoded-word decoder keeps its existing "emit the word verbatim"
/// behavior rather than silently guessing a charset for a header.
pub(super) fn decode_charset_opt(charset: &str, bytes: &[u8]) -> Option<String>;

/// The same decision with a LOSSY UTF-8 fallback on an unknown label,
/// for the RFC 2231 parameter path. See the three-fallback note below.
pub(super) fn decode_charset_lossy(charset: &str, bytes: &[u8]) -> String;
```

Three fallbacks for one shared decoder is two more than looks
comfortable, so each is named with the caller it belongs to and the
behavior it preserves. This is the whole behavior-preservation constraint
of brick 2, and it has two halves, not one:

- ENCODED WORDS (`decode_charset_opt`, `None`).
  `parse_encoded_word_inner` currently returns `None` on an unknown label
  (the `?` on `for_label`), which makes the whole encoded-word display
  verbatim per RFC 2047 section 6.3. Changing it changes IMAP subject
  decoding under a brick that claims to be a move.
- RFC 2231 PARAMETERS (`decode_charset_lossy`, lossy UTF-8).
  `decode_bytes_with_charset` at `crates/imap/src/types/rfc2231.rs:366`
  returns a `String` and falls back to `String::from_utf8_lossy` on an
  unknown label. The existing test
  `crates/imap/src/types/rfc2231_tests.rs:88
  unknown_charset_lossy_fallback` pins exactly that. Brick 2 repoints
  this call at `decode_charset_lossy`, NOT at `decode_charset_opt`
  (whose `None` has no meaning here) and NOT at `decode_charset` (whose
  windows-1252 guess would break the pinned test). An earlier draft of
  this spec pointed it at `decode_charset_opt`, which is a silent
  behavior change with a green-looking gate.
- BODY TEXT (`decode_charset`, windows-1252 + `Defect::UnknownCharset`).
  New code, no existing behavior to preserve, and the one path where a
  human is going to read the result. The policy is argued in the `Defect`
  doc comment.

Whether the 2231 path SHOULD eventually adopt the windows-1252 policy is
a real question and the answer is probably yes. It is not this brick's
question: this brick moves code, and a filename charset change belongs to
whoever is willing to update the test that pins it and say why.

While moving: `encoded_words.rs:237 hex_digit` and `rfc2231.rs:354
hex_val` are the same four-line function, and after this brick they land
in `words.rs` and `params.rs` in the same module tree. Keep ONE, in
`charset.rs` as `pub(super) fn hex_digit(b: u8) -> Option<u8>`, and have
both callers use it. Two copies of a hex-digit table in adjacent files is
the kind of duplication a move is the cheapest possible moment to remove.

Move `crates/imap/src/types/rfc2231.rs` to
`crates/types/src/mime/params.rs` and `rfc2231_tests.rs` to
`crates/types/src/mime/params_tests.rs`. Repoint its internal call to
`crate::codec::decode::decode_rfc2047` (line 224) at
`super::words::decode_encoded_words`, and its `decode_bytes_with_charset`
(line 366) at `super::charset::decode_charset_lossy`. Apply the
complexity rewrite described under `params.rs` above (keyed group
storage, base name recorded at group creation, aggregate allocation cap).

Repoint bifrost-imap:

- `codec/decode/mod.rs:44` (`mod encoded_words;`) is deleted and `:59`
  becomes the aliasing re-export named above, which keeps the ~20 call
  sites in `codec/decode/tests.rs` compiling untouched.
- `codec/decode/envelope_fetch.rs:4` and `codec/decode/bodystructure.rs:3`
  change from `use super::encoded_words::decode_rfc2047;` to
  `use super::decode_rfc2047;`. They import the module, not the
  re-export, so they do NOT ride through untouched.
- `codec/decode/bodystructure.rs:9` imports
  `bifrost_types::mime::decode_rfc2231_params`.
- `types/mod.rs:25` loses `pub(crate) mod rfc2231;`.
- `crates/imap/Cargo.toml` loses `encoding_rs`.

Gates:

- `brokkr test -p bifrost-types mime::params::tests` (the moved suite, 52
  cases). The filter is a substring match on the MODULE-QUALIFIED name,
  which is why it names the module path and not `rfc2231`: after the move
  the path is `mime::params::tests::*`, and only 7 of the 52 test names
  contain the string `rfc2231`. A `rfc2231` filter would report a green
  gate while exercising 13% of the suite - and
  `unknown_charset_lossy_fallback`, the case that pins the fallback
  decision argued above, is not among the 7.
- `brokkr test -p bifrost-types rfc2231_continuation_bomb_is_linear_and_capped`
  (new, in `params_tests.rs`: a header with several thousand distinct
  continuation base names completes, and one whose reassembled total
  exceeds `max_header_bytes` drops the overflow rather than allocating
  it)
- `brokkr test -p bifrost-imap spec_audit_rfc2047`
- `brokkr test -p bifrost-imap body_structure_rfc2231_params_end_to_end`
- `brokkr test -p bifrost-imap edge_bodystructure_rfc2231_continuation_reassembly`
- `brokkr check`

The three IMAP cases are the end-to-end proof that the decoders still
reach BODYSTRUCTURE and ENVELOPE decoding through their new home.

### Brick 3: the header model

Add `crates/types/src/mime/header.rs` with `HeaderMap` and its accessors,
plus the header-block splitter used by `parse.rs`:

```rust
/// Split raw octets at the first empty line into (header block, body).
///
/// Accepts CRLF and bare LF (real mail carries both; an IMAP `BODY[]`
/// response is CRLF but a locally stored `.eml` frequently is not).
///
/// When no empty line is found, records `Defect::MissingHeaderSeparator`
/// and decides by looking at the FIRST line:
///
/// - it contains a colon before any whitespace, i.e. it could be a
///   `field-name: value`: the whole input is the header block and the
///   body is empty. This is the truncated-fetch case (a `BODY[]<0.n>`
///   prefix that ended inside the headers) and it is the common one.
/// - otherwise: the whole input is the body and the header block is
///   empty. This is the bare-fragment case (a stored body with its
///   headers already stripped), where treating the text as headers would
///   throw the content away entirely.
///
/// A single rule cannot serve both and neither default is safe for the
/// other input, so the discriminator is explicit. Both branches are
/// pinned by name in the gates below.
pub(super) fn split_headers(raw: &[u8], limits: &MimeLimits, defects: &mut Vec<Defect>)
    -> (HeaderMap, Vec<u8>);
```

Unfolding: a continuation line (one starting with SP or HTAB) is joined
to the previous value with the newline removed and the leading whitespace
collapsed to a single space, per RFC 5322 section 2.2.3. A line with no
colon and no leading whitespace is skipped and does not terminate the
block (some agents emit a bare `From ` mbox separator line first).

The address-list parser handles: `addr-spec` alone; `display-name
<addr-spec>`; a quoted display name with escapes; comma separation with
commas inside quotes and inside angle brackets NOT splitting; a group
(`name: a@b, c@d;`) flattened to its members; RFC 2047 encoded-words in
the display name decoded AFTER quote removal. Malformed entries are
skipped rather than aborting the list.

The date parser handles RFC 5322 section 3.3 `date-time` including the
obsolete forms: optional day-of-week, one-or-two-digit day, three-letter
month, two- or four-digit year (two-digit: 0-49 maps to 2000-2049, 50-99
to 1950-1999, per RFC 5322 section 4.3), `hh:mm[:ss]`, numeric zone
`+hhmm`, and the obsolete alphabetic zones (`UT`, `GMT`, `EST`, `EDT`,
`CST`, `CDT`, `MST`, `MDT`, `PST`, `PDT`, and single-letter military
zones, which RFC 5322 section 4.3 says to treat as `-0000`). Result is a
`SystemTime`. Unparseable yields `None`.

The civil-date-to-instant conversion goes through `chrono` (added to
`crates/types/Cargo.toml` in brick 1): this brick hand-writes the RFC
5322 TOKENIZER, which is genuinely mail-specific, and does not hand-write
calendar arithmetic, which is not.

Gates (each name is a new test in `header_tests.rs`, and each is written
BEFORE the code it gates per point 5 of the spec contract):

- `brokkr test -p bifrost-types header_unfolds_continuation_lines`
- `brokkr test -p bifrost-types header_lookup_is_case_insensitive`
- `brokkr test -p bifrost-types header_get_all_preserves_wire_order`
- `brokkr test -p bifrost-types header_addresses_parse_display_name_and_group`
- `brokkr test -p bifrost-types header_addresses_ignore_comma_inside_quotes`
- `brokkr test -p bifrost-types header_addresses_decode_encoded_word_display_name`
- `brokkr test -p bifrost-types header_date_parses_rfc5322_and_obsolete_forms`
- `brokkr test -p bifrost-types header_date_two_digit_year_windows_correctly`
- `brokkr test -p bifrost-types header_message_ids_strip_angle_brackets`
- `brokkr test -p bifrost-types header_block_over_limit_records_defect`
- `brokkr test -p bifrost-types header_missing_separator_header_like_input_is_all_headers`
- `brokkr test -p bifrost-types header_missing_separator_bare_body_input_is_all_body`
- `brokkr check`

### Brick 4: transfer decoding, the parameter tokenizer, the entity parser

Add `transfer.rs`, extend `params.rs` with `ContentType`,
`parse_content_type`, `parse_disposition`, and the tokenizer, and add
`parse.rs` with `MimePart`, `PartBody`, `ParsedMessage`, `parse_message`,
`parse_message_with_limits`. (`MimeLimits` and `Defect` already exist,
from brick 1.)

Two rules here are easy to lose because they live in the parser rather
than in the selector:

- A part whose `Content-Transfer-Encoding` is `TransferEncoding::Unknown`
  gets `content_type` forced to `application/octet-stream` per RFC 2045
  section 6.4, with the declared type still readable from `headers`.
- The multipart recursion threads a `default_type: ContentType` for
  children with no `Content-Type` header: `message/rfc822` under a
  `multipart/digest` parent (RFC 2046 section 5.1.5),
  `text/plain; charset=us-ascii` everywhere else (RFC 2045 section 5.2).

Multipart splitting: scan the body for a line equal to `--<boundary>`
(delimiter) or `--<boundary>--` (close-delimiter), allowing trailing
whitespace on the line per RFC 2046 section 5.1.1, at a line start only.
Octets before the first delimiter are the preamble and are discarded;
octets after the close-delimiter are the epilogue and are discarded. A
body with no close-delimiter yields the parts found plus
`Defect::Truncated`. A `multipart/*` with no `boundary` parameter is kept
as a single leaf plus `Defect::MissingBoundary`. Recursion is bounded by
`max_depth` and the running part count by `max_parts`; either bound
converts the offending subtree into an undecoded leaf and records its
defect.

Quoted-printable decoding follows RFC 2045 section 6.7: `=XX` hex pairs,
`=` at end of line as a soft break (both `=\r\n` and `=\n`), trailing
whitespace on a line stripped before the line break, a malformed `=XY`
emitted literally with `Defect::MalformedQuotedPrintable`.

Base64 decoding skips CR, LF, SP, HTAB and any character outside the
alphabet, and tolerates absent padding by decoding the final partial
group; a group of length 1 is dropped. Either leniency records
`Defect::MalformedBase64`.

Gates (all new, in `parse_tests.rs`, `transfer_tests.rs`,
`params_tests.rs`):

- `brokkr test -p bifrost-types parse_plain_text_message_without_mime_version`
- `brokkr test -p bifrost-types parse_multipart_alternative_yields_two_leaves`
- `brokkr test -p bifrost-types parse_nested_multipart_mixed_related_tree`
- `brokkr test -p bifrost-types parse_discards_preamble_and_epilogue`
- `brokkr test -p bifrost-types parse_missing_close_delimiter_records_truncated`
- `brokkr test -p bifrost-types parse_multipart_without_boundary_is_a_leaf`
- `brokkr test -p bifrost-types parse_depth_limit_stops_recursion`
- `brokkr test -p bifrost-types parse_part_limit_stops_expansion`
- `brokkr test -p bifrost-types parse_message_rfc822_is_embedded_not_flattened`
- `brokkr test -p bifrost-types parse_accepts_bare_lf_line_endings`
- `brokkr test -p bifrost-types transfer_base64_tolerates_missing_padding`
- `brokkr test -p bifrost-types transfer_base64_skips_non_alphabet_bytes`
- `brokkr test -p bifrost-types transfer_quoted_printable_soft_line_breaks`
- `brokkr test -p bifrost-types transfer_quoted_printable_strips_trailing_whitespace`
- `brokkr test -p bifrost-types transfer_quoted_printable_malformed_escape_is_literal`
- `brokkr test -p bifrost-types transfer_unknown_encoding_passes_through_with_defect`
- `brokkr test -p bifrost-types unknown_encoding_forces_application_octet_stream`
- `brokkr test -p bifrost-types digest_child_without_content_type_defaults_to_message_rfc822`
- `brokkr test -p bifrost-types content_type_tokenizer_handles_quoted_semicolon`
- `brokkr test -p bifrost-types content_type_tokenizer_reassembles_rfc2231_continuation`
- `brokkr test -p bifrost-types content_type_missing_defaults_to_text_plain_us_ascii`
- `brokkr test -p bifrost-types charset_unknown_label_falls_back_to_windows1252`
- `brokkr test -p bifrost-types text_over_limit_truncates_on_char_boundary`
- `brokkr test -p bifrost-types filename_strips_path_separators`
- `brokkr check`

Two of these deserve naming as the security bricks the TODO's imap-T4
entry gestures at: `parse_part_limit_stops_expansion` bounds a
boundary-bomb, and `text_over_limit_truncates_on_char_boundary` bounds
the charset expansion the imap-T4 note describes ("a 100 KB base64
payload in a legacy multi-byte charset expands several-fold and nothing
caps the resulting subject length"). This spec does not close imap-T4,
which also covers BODYSTRUCTURE fuzzing, but it removes that entry's
unbounded-expansion concern from the NEW code path.

### Brick 5: body selection and the attachment shape

Add `select.rs`. Retype `Message::attachments` and add
`MessageAttachment` / `AttachmentSource` to `hydration.rs`. Update
`lib.rs` re-exports. Migrate JMAP, Graph, and Google construction as
described in "The other three crates".

This is one landing rather than two because the retype breaks three
crates at once: a `Message` cannot hold the old and new attachment shape
simultaneously, and splitting it produces a boundary where
`brokkr check` is red, which point 6 of the spec contract forbids.

Gates:

- `brokkr test -p bifrost-types select_alternative_prefers_last_representation`
- `brokkr test -p bifrost-types select_related_start_part_is_body_rest_are_attachments`
- `brokkr test -p bifrost-types select_signed_takes_first_child_signature_is_attachment`
- `brokkr test -p bifrost-types select_encrypted_yields_no_body_and_two_attachments`
- `brokkr test -p bifrost-types select_inline_flag_set_by_cid_reference_in_html`
- `brokkr test -p bifrost-types select_text_part_with_filename_is_an_attachment`
- `brokkr test -p bifrost-types select_metadata_only_still_reports_size`
- `brokkr test -p bifrost-types select_embedded_message_is_an_attachment_not_a_body`
- `brokkr test -p bifrost-types select_embedded_attachment_bytes_are_the_original_octets`
- `brokkr test -p bifrost-types select_metadata_only_does_not_decode_base64`
- `brokkr test -p bifrost-jmap attachment_from_part_fills_cid_and_disposition`
  (new: pins both the corrected getter names and the fact that
  `BodyProperty::Cid` / `Disposition` are now requested, which is the
  half of the JMAP migration that a shape-only test misses)
- `brokkr test -p bifrost-jmap rebase_foreign` (the `Blob`-variant-only
  id rewrite)
- `brokkr test -p bifrost-graph hydrate`
- `brokkr test -p bifrost-google attachments_for_message`
- `brokkr test -p bifrost-google hydrate_full_reports_attachment_metadata_without_bytes`
  (new: pins the widened `Full | FullWithBlobs` gate)
- `brokkr check`

`brokkr test` takes `<NAME>` as a REQUIRED positional (see `brokkr test
--help`), so a bare `brokkr test -p bifrost-jmap` is not a runnable
command. An earlier draft listed two such gates. Where a whole-crate
sweep really is what is wanted, that is what `brokkr check` at the end of
the brick already does.

### Brick 6: wire bifrost-imap, and invert the defect test

Rewrite `fetch_to_message` and `attrs_for_hydration` as specified. Then
REPLACE the existing IMAP test
`full_hydration_puts_the_whole_raw_message_in_body_text`
(`crates/imap/src/account/pim.rs:2971`) with
`full_hydration_decodes_multipart_into_text_html_and_attachments`. The
old test pins the DEFECT and must not survive the fix; leaving it as an
`#[ignore]` or renaming it without inverting its assertions would leave a
test asserting the wrong behavior in the tree.

The replacement feeds a canned `FetchResponse` whose `BODY[]` section
carries a `multipart/mixed` message with a `multipart/alternative`
(quoted-printable `text/plain` with a non-ASCII character, base64
`text/html`) plus a base64 `application/pdf` attachment with an RFC 2231
encoded filename, and asserts: `body_text` is the decoded plain text and
contains no boundary marker and no `Content-Type` line; `body_html`
carries the decoded HTML; `attachments` has one entry with the decoded
filename, `content_type == "application/pdf"`, and, under
`FullWithBlobs`, `AttachmentSource::Inline` holding the decoded octets;
under `Full`, `AttachmentSource::None` with the same `size`.

Three further IMAP cases, all hermetic, all driven through the same
canned-`FetchResponse` seam:

- `preview_projection_extracts_text_from_a_multipart_prefix`: a
  `multipart/alternative` message fed as a `Preview(200)` fetch yields
  `body_text` that is decoded prose containing no boundary marker. This
  is the defect the preview lane change exists to fix and nothing else
  pins it end to end.
- `preview_projection_requests_at_least_the_caller_limit`: a
  `Preview(256 * 1024)` produces a `BodySection` whose `partial` length
  is 256 KiB, not `PREVIEW_FETCH_BYTES`.
- `truncated_fetch_marks_the_message_incomplete`: a `FullWithBlobs` fetch
  parsed under a deliberately tiny `MimeLimits` yields
  `Message::incomplete == true` and an attachment with
  `truncated == true`. This is the only test that proves a parser defect
  survives the hydration boundary at all.

Also update `crates/imap/src/account/pim.rs`'s
`attrs_for_hydration` test cases at lines 2906-2928 for the new preview
attribute, and extend `hydration_headers_projection_has_no_body`
(line 2948 area) unchanged in intent.

Then update `reference/types.md` (file map entry for `mime.rs` becomes
the `mime/` tree; the "Hydration and blobs" section gains the
`MessageAttachment` / `AttachmentSource` split, the `Message::incomplete`
flag, and the reason for both) and
`reference/imap.md` (hydration section: the parser is now the body path,
the preview fetch shape changed, `encoded_words.rs` and `rfc2231.rs` now
live in bifrost-types, the blob lane is still `Unsupported` but decoded
bytes reach `FullWithBlobs` inline). Per the workspace commit rules the
markdown lands WITH this brick, not separately.

`reference/types.md` must also draw one naming line explicitly, because
after this spec bifrost-types has THREE "attachment" families and two of
them have an `Inline`:

- `compose::AttachmentHandle` / `compose::AttachmentInline` - OUTBOUND,
  what the caller hands the serializer to send.
- `hydration::MessageAttachment` / `AttachmentSource::Inline` - INBOUND,
  what a hydrated `Message` carries to a consumer.
- `mime::DecodedAttachment` - the PARSER's intermediate, produced by
  `select_body` and mapped onto `MessageAttachment` by each protocol
  crate. Not a hydration type; it knows nothing about blobs.

None of these is wrong and renaming is not proposed, but a reader who
meets `AttachmentInline` and `AttachmentSource::Inline` in the same crate
without that paragraph will guess they are related. They are not.

Gates:

- `brokkr test -p bifrost-imap full_hydration_decodes_multipart_into_text_html_and_attachments`
- `brokkr test -p bifrost-imap preview_projection_extracts_text_from_a_multipart_prefix`
- `brokkr test -p bifrost-imap preview_projection_requests_at_least_the_caller_limit`
- `brokkr test -p bifrost-imap truncated_fetch_marks_the_message_incomplete`
- `brokkr test -p bifrost-imap attrs_for_hydration`
- `brokkr test -p bifrost-imap hydration`
- `brokkr check --all`

`brokkr check --all` rather than `brokkr check` on the final brick: the
changed-files scope has been narrowing across six landings and the last
gate should see every diagnostic in the workspace, uncapped.

## What this spec does not leave behind

- No `#[cfg(feature = ...)]` on the parser, no env-var switch, no
  "legacy body path" retained alongside the new one. `fetch_to_message`
  has exactly one body path after brick 6.
- No dead re-export shims in bifrost-imap beyond the single aliasing
  `pub(crate) use` at `codec/decode/mod.rs:59`, which exists to keep ~20
  existing test call sites compiling and is a real name binding, not a
  compatibility layer for external callers (the crate has none for that
  symbol; it was `pub(crate)`).
- No new THIRD-PARTY dependency. `encoding_rs` moves from
  `crates/imap/Cargo.toml` to the workspace table and
  `crates/types/Cargo.toml`; `chrono` is added to
  `crates/types/Cargo.toml` from the workspace table where it already
  sits and where bifrost-imap already consumes it. Neither adds a crate
  to the build graph. `base64` and `bytes` are already there.
- No performance record, no benchmark harness, no integration or
  mock-server gate. Bifrost keeps none and adds none; correctness is the
  measured axis and `brokkr check` plus the named cases above are the
  measurement.

## Review disposition

Two independent reviews (R1, Opus; R2, codex xhigh) were run against the
first draft. Every finding from both was re-verified against the tree
before disposition. All accepted findings are folded into the body above,
not appended here; this section exists so a later reader knows what was
weighed and does not re-litigate it.

Accepted, and where each landed:

- `SelectOptions` unexported while `select_body_with` is public (both
  reviews) - module tree, export list.
- `HeaderName` exported but never defined (R2) - export list.
- `decode_rfc2231_params` unexported while IMAP is told to import it
  (R2) - export list.
- `PartBody` missing `#[non_exhaustive]` (R2) - type definition.
- Deleting the `_ => {}` arm is a compile error, `#[non_exhaustive]` is
  per-crate not per-workspace (R1) - IMAP section, arm retained with the
  reason.
- `envelope_fetch.rs:4` / `bodystructure.rs:3` import the MODULE, so the
  aliasing re-export does not keep them compiling; `mod encoded_words;`
  at `mod.rs:44` also needs deleting (R1) - deletions list and brick 2.
- Repointing the RFC 2231 charset path at `decode_charset_opt` changes
  behavior and breaks the pinned `unknown_charset_lossy_fallback` (R1) -
  third fallback `decode_charset_lossy` added, the three-way split
  argued explicitly.
- Brick 2's `rfc2231` test filter matches 7 of 52 cases (R1) - gate is
  now the module path.
- `MimePart::text()` cannot honor `max_text_bytes` or record defects from
  `&self` (both) - `text_with(limits, defects)` added, `text()`
  documented as the default-limit convenience.
- `is_attachment()` cannot know about `cid:` references (both) - clause
  removed from the doc, ownership pinned on selection rule 9.
- Rule 5 asks for octets `PartBody::Embedded` does not retain (both) -
  `Embedded { raw, message }`.
- Bricks 2 and 3 use `Defect` / `MimeLimits` before brick 4 defines them
  (R2) - `limits.rs`, landed in brick 1.
- The reused RFC 2231 decoder is quadratic (`rfc2231.rs:73` and `:165`)
  and this spec adds an attacker-controlled caller (R2) - mandatory
  rewrite specified, with a gate.
- Truncation defects vanish at the hydration boundary (R2) -
  `Message::incomplete`, `MessageAttachment::truncated`,
  `DecodedAttachment::truncated`, `DecodedBody::defects`.
- Unknown transfer encoding must force `application/octet-stream`, RFC
  2045 section 6.4 (R2) - `TransferEncoding::Unknown` doc and brick 4.
- `multipart/digest` default child type is `message/rfc822`, RFC 2046
  section 5.1.5 (R2) - selection rule 3a and the `default_type` thread.
- `multipart/encrypted` cannot share the signed rule, RFC 1847 section
  2.2 (R2) - selection rule 4a.
- windows-1252 is a compatibility policy, not an RFC 2046 fallback (R2) -
  `Defect::UnknownCharset` doc relabeled.
- The preview lane ignores a caller's `N > 64 KiB` (both) - `max(limit,
  PREVIEW_FETCH_BYTES)`, and the 64 KiB floor relabeled a heuristic.
- The spec contradicted itself on the missing-header-separator case (R2)
  - `split_headers` now discriminates on the first line, both branches
  gated.
- JMAP getters are `content_id()` / `content_disposition()`, and the
  request does not ask for `BodyProperty::Cid` / `Disposition` (R2) -
  JMAP migration, with a gate.
- Providers disagree about whether `Full` yields attachment metadata
  (R2) - "One cross-provider projection rule".
- `brokkr test -p <crate>` without a `<NAME>` is not runnable (R2) -
  brick 5 gates named.
- Missing tests: end-to-end preview, provider attachment metadata,
  truncation reaching `Message` (R2) - added to bricks 5 and 6.
- `Full` decoding attachments only to discard them (R1) -
  `DecodedAttachment::size` doc now forbids decode-and-discard on the
  metadata lane, with a gate.
- Two hex-digit helpers converging in one module tree (R1) - folded into
  `charset.rs`.
- Three "attachment" families in one crate (R1) - `reference/types.md`
  must draw the line; specified in brick 6.
- The date parser is hand-rolled and unpriced (R1) - `chrono` taken, with
  the reasoning.
- Stale line counts, `mime.rs` 940 and `rfc2231_tests.rs` 727 (R1) -
  corrected.

Rejected:

- R2's characterization that the spec "claims a linear hostile-input
  bound" for the reused RFC 2231 decoder. It does not; the linearity
  claims in the draft were about `ENCODED_WORD_SCAN_LIMIT` and about the
  new parser's caps, and nothing in the draft asserted anything about
  RFC 2231 complexity. The underlying finding - that the decoder IS
  quadratic and that this spec newly exposes it to attacker-controlled
  header text - is correct and accepted above; only the "the plan claims
  otherwise" framing is dropped, because a future reader should not go
  hunting for a claim that was never made.
- R2's "the plan is not ready for implementation" verdict, as a verdict.
  Every specific finding under it was accepted; none of them touched the
  two structural decisions the reviews were called to test (the parser
  belongs in bifrost-types, and `Message::attachments` gets a two-lane
  source type), and both reviews independently endorsed those. The
  findings were spec bugs, which is what a spec review is for.
