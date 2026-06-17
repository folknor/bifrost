# DAV body parsing robustness: bifrost hand-rolled vs ratatoskr/calcard

Date: 2026-06-17
Scope: read-only gap analysis. No code changed.

## Headline conclusion

bifrost's CalDAV/CardDAV body handling is a pair of hand-rolled
"small projection" modules (`crates/caldav/src/ical.rs`,
`crates/carddav/src/vcard.rs`) that do their own line unfolding, escape
handling, parameter parsing, and serialization. Ratatoskr does the same
job through `calcard` - a streaming, `Cow`-tokenizing, fuzz+MIRI-tested
parser - after a 4-round review and a 9-crate survey.

The bifrost parsers are NOT robustness-equivalent to what ratatoskr
landed. The gap is real but narrower than "rewrite everything", because
bifrost is a thin client projection: it does not expand RRULE, does not
resolve VTIMEZONE to offsets, and re-serializes a constrained property
set while passing unmodeled lines through verbatim. Within that reduced
surface, the dominant defects are:

1. A genuine round-trip data-loss bug on the patch/preserve path: bifrost
   re-folds and partly re-interprets *preserved* (unmodeled) lines, and
   on the caldav side it unfolds-then-refolds every line including ones
   it never modeled, so an input that was correct can come back subtly
   different (or corrupt) - the exact lossy-round-trip class the survey
   flagged against the naive `icalendar` crate.
2. The unfolders only treat leading SPACE/TAB as continuation but join
   with `trim_start()`, which silently eats *all* leading whitespace of a
   folded segment, not just the single fold octet. RFC 5545/6350 say
   strip exactly one leading WSP. This corrupts values whose fold point
   landed before a real space.
3. No quoted-parameter / multi-parameter / `:`-in-quoted-value handling
   on the parse side: a `TZID="A/B:C"` or `CN="Last, First"` parameter is
   mis-split on the first `:` or `;`.
4. vCard structured-value and version handling is shallow: `ADR`/`N`
   component indexing is fixed-position and lossy on re-serialize; vCard
   3 vs 4 `TYPE`/`PREF` differences are not modeled; group prefixes
   (`item1.TEL`) are dropped on the parse side and mangled on patch.

None of these are "wrong AST shape" problems - they are concrete parsing
bugs that calcard does not have. calcard is NOT currently a bifrost
dependency (absent from `Cargo.lock`) and is NOT checked out under
`research/` (only ratatoskr is), so adopting it is a real new-crate
decision, weighed at the end.

Finding counts:

- iCal: 2 bug, 6 gap, 3 smell, 1 nit
- vCard: 3 bug, 5 gap, 2 smell, 1 nit
- Cross-cutting: 1 gap, 1 smell

---

## iCal findings (`crates/caldav/src/ical.rs`)

### bug (H): re-folding preserved lines mutates verbatim content

`replace_first_vevent_properties` (ical.rs:688) calls `unfold_lines`
on the raw input, passes every non-replaced line straight through, then
`fold_ical_lines` (ical.rs:756) re-folds the whole document. This means
every untouched/unmodeled line is unfolded and re-folded on every update.
Two concrete losses:

- An input folded at a different column comes back folded at column 75.
  That alone is legal, but combined with the unfold bug below it can
  drop characters.
- `unfold_lines` joins continuations with `line.trim_start()`
  (ical.rs:344), so a continuation segment that legitimately began with
  more than one space (a value whose fold landed mid-run-of-spaces, e.g.
  a DESCRIPTION containing `"a:    b"` folded after `a:`) loses those
  spaces permanently. The survey's whole point about `icalendar`'s lossy
  round-trip is exactly this failure class; bifrost has its own variant.

The `patch_preserves_unmodeled_vevent_properties` test only checks short
single-line properties, so it never exercises the fold/unfold round trip
on a long preserved value. This is the highest-severity finding because
the crate's headline promise is "preserve untouched/unmodeled lines."

### bug (H): unfolder strips all leading whitespace instead of one octet

`unfold_lines` (ical.rs:338-351, and the identical copy in
vcard.rs:366-379): RFC 5545 sec 3.1 and RFC 6350 sec 3.2 say a folded line
is rejoined by deleting the CRLF and *exactly one* following WSP. bifrost
deletes the CRLF then `trim_start()`s the entire continuation, deleting
every leading space/tab. Any value where a space sits right after a fold
boundary loses that space on read. On a CalDAV resource this corrupts the
projected SUMMARY/DESCRIPTION/LOCATION text and, worse, feeds the corrupted
text back as `raw_ical` (ical.rs:81) so a later patch re-serializes the
corruption.

### bug-adjacent / gap (H): `raw_ical` stores the as-received bytes, but projection reads through the buggy unfolder

`event_from_ical` stores `raw_ical: Some(data.to_string())` verbatim
(good), but the patch path re-derives everything through `unfold_lines`.
So the verbatim copy is only safe as long as no patch occurs; the first
update launders it through the lossy unfold/refold. calcard keeps
tokenization `Cow`-borrowed and never round-trips untouched bytes through
a lossy normalizer.

### gap (H): parameter parsing cannot handle quoted values containing `:` or `;`

`parse_vevent` (ical.rs:301) splits each line on the *first* `:`
(`line.split_once(':')`), then splits the name part on `;`. A parameter
value may be a quoted-string per RFC 5545 sec 3.2 that legally contains
`:` and `;`, e.g. `ATTENDEE;CN="Doe, John":mailto:...` or
`DTSTART;TZID="Custom:Zone":...`. bifrost splits on the first `:`,
truncating the quoted parameter and mis-reading the value. Note the
asymmetry: the *serializer* `escape_param` (ical.rs:795) DOES quote params
containing `:`/`;`/`,`, and there is even a test
(`escapes_text_and_params_without_raw_line_breaks`) asserting
`DTSTART;TZID="Europe/Oslo:Main":` is emitted - but the parser would
then mis-read that very output on the next sync. So bifrost can emit
vCard/iCal it cannot itself round-trip. calcard handles quoted params in
the tokenizer.

### gap (M): duplicate-property policy is silent first-wins with no diagnostic

`Props::first_with_name` (ical.rs:368) returns the first match; there is
no detection or logging of duplicate DTSTART/DTEND/UID. The survey calls
out that calcard collects duplicates and ratatoskr added
`pick_datetime_entry` (ical/mod.rs:473) with an explicit precedence
ladder (VALUE=DATE > TZID > UTC-offset > floating) plus a WARN when more
than one DTSTART is present - precisely because real Outlook/bridge
emitters send a TZID DTSTART paired with a floating fallback DTSTART.
bifrost takes whichever comes first in document order, so the same event
can project to two different times depending on emitter ordering. This is
a documented real-world shape, not a theoretical one.

### gap (M): TZID datetime is reduced to a UTC string, losing the wall-clock

`format_ical_time` (ical.rs:405) for a `TZID=...` value with no offset
appends a `Z` (ical.rs:430), so `DTSTART;TZID=Europe/Oslo:20260602T120000`
projects to `2026-06-02T12:00:00Z` with `timezone=Some("Europe/Oslo")`.
The numeric value is now a *wall-clock time mislabeled as UTC*. The test
`parses_tzid_datetime_as_rfc3339_with_timezone_metadata` enshrines this:
it asserts `start.value == "2026-06-02T12:00:00Z"`. Downstream consumers
that trust the `Z` get a time wrong by the zone offset. Ratatoskr
explicitly does NOT do this: it resolves the wall-clock through the zone
(`resolve_local_to_timestamp`, ical/mod.rs:411) to a real instant and
stores the resolved IANA name separately. bifrost's contract is "client
projection, expansion lives downstream", so storing wall-clock + TZID is
defensible - but tagging it `Z` is actively misleading and the value
field should not carry a false UTC marker. (If the downstream contract is
"value is wall-clock, timezone names the zone", then the `Z` is a bug;
either way the `Z` is wrong.)

### gap (M): VTIMEZONE emitted as a fixed +0000 stub regardless of the real zone

`push_vtimezones` (ical.rs:592) emits, for any TZID, a VTIMEZONE whose
STANDARD block is hardcoded `TZOFFSETFROM:+0000 / TZOFFSETTO:+0000`. So a
created event in `Europe/Oslo` ships a VTIMEZONE claiming Oslo is UTC. The
reference doc (`caldav.md`) is honest about this ("conservative
fixed-offset stubs"), and a strict server may re-resolve the TZID by name
and ignore the stub, but a server that trusts the supplied VTIMEZONE will
place the event an hour or two off. ratatoskr leans on calcard's
TzResolver + Windows alias table; bifrost has no zone data at all. Windows
/ Exchange zone names (`"W. Europe Standard Time"`) are passed through as
opaque TZID strings with no IANA mapping on either parse or emit.

### gap (M): no Windows/Exchange TZID alias handling

Related to the above but distinct: on *parse*, a `TZID=W. Europe Standard
Time` is stored verbatim as the timezone string. bifrost never maps
Microsoft zone names to IANA. The survey's catalog item #1 is exactly
"vendor the CLDR windowsZones.xml into a phf map"; bifrost would need the
same table to be robust against Exchange-fronted CalDAV (which the project
explicitly targets via Graph, and CalDAV-against-Exchange is plausible).

### gap (M): RDATE/EXDATE multi-value and VALUE=DATE not modeled

`event_from_ical` collects RDATE/EXDATE as raw whole-value strings
(ical.rs:45-53) and re-emits them verbatim. RFC 5545 allows a single
RDATE/EXDATE property to carry a comma-separated list of dates, and
RDATE can carry PERIOD values. bifrost treats the entire property value
as one opaque token. For pure pass-through round-trip this is lossless
(the string is preserved), so it is a modeling gap rather than corruption
- but any consumer reading `recurrence.rdate` as "one entry per date"
gets a single multi-date blob. RECURRENCE-ID is likewise stored as a raw
string with no canonicalization; ratatoskr built
`extract_recurrence_id_canonical` (ical/mod.rs:554) with four explicit
wall-clock forms precisely so master+override keys do not collide across
hosts. bifrost only reads RECURRENCE-ID off the *first* VEVENT, which is
the master and normally has none (see next).

### smell (M): only the first VEVENT is ever projected; overrides are invisible

`parse_vevent` breaks at the first `END:VEVENT` (ical.rs:314). A recurring
series with override instances (each its own VEVENT with a RECURRENCE-ID)
projects only the master. The test
`multi_vevent_projection_reads_only_first_vevent` enshrines this as
intended. For a read-mostly client this is a deliberate scope choice, and
the patch path does preserve override VEVENTs verbatim
(`scalar_patch_preserves_override_vevents`) and refuses recurrence patches
when overrides exist (`has_recurrence_override_vevent`, ical.rs:724). So
this is consistent, but it is strictly less capable than calcard, which
surfaces every VEVENT. Flagging as a smell, not a bug, because the
behavior is intentional and guarded.

### smell (L): ordering-dependent unescape chain (mitigated, but fragile)

`unescape_text` (ical.rs:787) is a fixed sequence of `.replace()` calls:
`\n` then `\,` then `\;` then `\\`. Because `\\` is unescaped LAST, the
classic `icalendar` bug (where `\\,` wrongly decodes to a literal comma)
is mostly avoided here for the forward direction - but it is still a
replace-chain, not a single left-to-right scan, so it mis-handles
adjacency like `\\n` (intended: backslash + `n`) which this chain turns
into backslash + newline. The vCard side (vcard.rs:465) correctly uses a
single-pass scanner; the iCal side does not. The survey explicitly named
the multi-pass `replace()` chain as the icalendar bug; bifrost's iCal side
has the same anti-pattern even if the specific ordering dodges the comma
case.

### nit (L): `escape_text` strips CR but maps lone LF to `\n` only after CRLF removal

`escape_text` (ical.rs:778) does `.replace('\r', "")` then
`.replace('\n', "\\n")`. A bare CR (old-Mac line ending) is deleted
rather than converted, silently joining two lines. Minor; the vCard side
handles this better (vcard.rs:455 maps `\r\n` and `\r` to `\n` first).

---

## vCard findings (`crates/carddav/src/vcard.rs`)

### bug (H): patch path drops vCard group prefixes and re-folds preserved lines

`vcard_from_patch` (vcard.rs:58) unfolds the source, keeps non-replaced
lines, and re-folds with `fold_vcard_lines` (vcard.rs:437). Same
fold/unfold round-trip hazard as the iCal side (same `trim_start()` bug
in the shared-shape `unfold_lines` at vcard.rs:366). Additionally,
`property_name` (vcard.rs:212) strips a group prefix
(`item1.TEL` -> `TEL`) only to *decide replacement*, but the preserved
line is pushed verbatim - good - while any line the patch *replaces* is
re-emitted with NO group prefix. So patching the emails on a card whose
EMAIL lines were grouped (`item1.EMAIL` / `item1.X-ABLabel:Work`, the
standard Apple Contacts shape) detaches the X-ABLabel from its now
un-grouped EMAIL, corrupting the label association. calcard models groups
as first-class.

### bug (M): ADR re-serialization is fixed-position and silently drops PO-box / extended-address

`append_addresses` (vcard.rs:150) always emits
`ADR...:;;{street};{locality};{region};{postal};{country}` - it
hardcodes the first two ADR components (post-office-box, extended
address) to empty. The parser `address_from_adr` (vcard.rs:302) reads
street from components 0,1,2 (PO box + extended + street, flattened
together) and never preserves PO box / extended separately. So a card
with a real PO box or extended address (`ADR:PO Box 1;Suite 5;1 Main
St;...`) loses the PO box and suite on the first patch that touches
addresses, because the model has no slot for them and the re-serializer
zeroes those positions. This is structured-value data loss on update.

### bug (M): inline PHOTO base64 is not folded correctly and 3.0 vs 4.0 syntax is conflated

`append_inline_photo` (vcard.rs:182) emits `PHOTO;ENCODING=b;TYPE=PNG:<base64>`.
That is vCard 3.0 syntax. The create path writes `VERSION:4.0`
(vcard.rs:51), where the correct inline form is
`PHOTO:data:image/png;base64,<...>` with no `ENCODING=b`. So a 4.0 card
gets a 3.0 PHOTO line - many strict 4.0 consumers will not decode it.
Separately, the base64 payload is fed through the generic
`fold_vcard_lines` which folds on a 75-*octet* boundary counting
`char` byte-length but NOT accounting for the leading fold space the same
way RFC requires for binary continuations; base64 is ASCII so it mostly
works, but the photo TYPE is round-tripped as the media subtype
(`JPEG`/`PNG`) rather than a proper `image/jpeg` media-type in 4.0. The
parse side accepts `ENCODING=b`, `ENCODING=BASE64`, `VALUE=BINARY`
(vcard.rs:496) - reasonably tolerant on read - but the write side is
version-inconsistent.

### gap (H): quoted parameter values mis-split (same as iCal)

`parse_vcard` (vcard.rs:240) splits on first `:` then on `;`, identical
to the iCal bug. A `TYPE="work,home"` or any quoted param containing `:`
or `;` is mis-parsed. `type_from_params` (vcard.rs:396) does
`trim_matches('"')` but only after the line was already split on the
wrong `:`. calcard tokenizes quoted params correctly.

### gap (M): vCard 3.0 vs 4.0 TYPE/PREF semantics not distinguished

`is_primary` (vcard.rs:412) treats bare `PREF` (3.0 style) and `PREF=1`
(4.0) as primary, and `type_param` (vcard.rs:381) always emits `PREF=1`
(4.0 form) even though it can be writing into a card that the patch path
otherwise leaves as `VERSION:3.0` (the preserved VERSION line is never
rewritten). So a 3.0 card gets 4.0 `PREF=1` parameters mixed in. Also 3.0
`TYPE=HOME,WORK` comma-lists vs 4.0 repeated `TYPE=` are not handled:
`type_from_params` returns only the first match and lowercases it, so a
multi-typed entry loses all but one type. The survey notes calcard
"handles both vCard 3.0 and 4.0" as a baseline expectation.

### gap (M): only the first EMAIL/TEL/etc. type is kept; multi-value TYPE collapses

`type_from_params` (vcard.rs:396) uses `find_map` - first hit wins - so
`TEL;TYPE=cell;TYPE=voice` keeps only `cell`. On re-serialize only one
TYPE is written back. Round-trip-lossy for multi-typed entries.

### gap (M): empty-vs-absent not distinguished, and empty values are dropped

`parse_vcard` guards every field with `if !value.is_empty()`
(vcard.rs:249 onward), so a present-but-empty `NOTE:` is indistinguishable
from an absent NOTE. This is the survey's review-findings #47 - the one
thing only caldata-rs solves and that even calcard cannot. bifrost has
the same limitation as calcard here, so it is not *worse* than what
ratatoskr landed on this specific axis - flagged for completeness.

### gap (L): ORG component join is lossy

`parse_vcard` ORG handling (vcard.rs:263) splits the ORG structured value
on `;`, drops empties, and joins surviving parts with a single space into
one `name` string (`Acme;R&D;Lab` -> `Acme R&D Lab`). The unit/department
structure is gone, and on re-serialize `append_organizations`
(vcard.rs:141) writes the flattened name as a single ORG component, so the
original `ORG:Acme;R&D;Lab` cannot be reconstructed. Structured-value
loss, lower severity because ORG sub-structure is rarely consumed.

### smell (M): `N` is parsed for display-name fallback but never preserved as a model field

`display_name_from_n` (vcard.rs:327) builds a display name from `N` when
`FN` is absent, but `N` itself is not in `ParsedVCard`. On the patch path
`N` is preserved verbatim (it is not in `should_replace_property`,
vcard.rs:199), which is correct. But on the *create* path
`vcard_from_create` (vcard.rs:47) never emits `N` at all - it writes only
`FN`. vCard 3.0 requires `N` (it is mandatory in 3.0); a created 4.0 card
omitting `N` is legal in 4.0 but some servers/clients reject 3.0-style
cards without it. Minor because creates default to 4.0.

### smell (L): photo URL detection only matches http(s)

`is_url` (vcard.rs:491) only recognizes `http://` / `https://`. A
`PHOTO;VALUE=URI:data:...` or other URI scheme falls through to the
inline-photo arm, where `inline_photo` requires `ENCODING=b` and returns
None otherwise, so a `data:` URI photo is silently dropped entirely.

### nit (L): `fold_vcard_lines` does not count the continuation space in the octet budget

`fold_vcard_lines` (vcard.rs:437) checks `current.len() + ch.len_utf8() > 75`
but, unlike the iCal `fold_ical_lines` (which tracks `prefix = 1` for the
fold space, ical.rs:768), does NOT reserve the leading-space octet on
continuation lines. So vCard continuation lines can hit 76 octets. The
iCal side got this right and has a regression test
(`fold_counts_continuation_space_in_octet_budget`); the vCard side does
not. Trivial fix, real inconsistency.

---

## Cross-cutting findings

### gap (M): output is not byte-stable across re-serialization

Because both serializers normalize folding to column 75, drop CR,
re-order nothing but re-fold everything, two serializations of the same
logical event/card differ from the input bytes (and a patched resource
differs from an unpatched fetch of the same resource). The survey flags
content-hash instability as a real concern (the `icalendar` DTSTAMP/UID
synthesis issue). bifrost is better than `icalendar` (it does not
synthesize timestamps), but it still does not guarantee
re-serialize(parse(x)) == x for the preserved portion, which undercuts
any future etag/content-hash dedup. calcard's `Cow` tokenizer preserves
untouched bytes.

### smell (L): two identical hand-rolled `unfold_lines` copies, two escape implementations

`unfold_lines` is duplicated verbatim in ical.rs:338 and vcard.rs:366,
and there are two different `escape_text`/`unescape_text` pairs with
subtly different behavior (the iCal unescape is a replace-chain, the vCard
unescape is a single-pass scanner). Any fix to the unfold/escape bugs has
to be made in two places and they have already drifted. The DAV XML
`parse.rs` files are separately duplicated too but are out of body-parsing
scope and are in good shape (the review-findings doc lists the ratatoskr
equivalents as verified non-issues).

---

## Note on the DAV XML parsers (`parse.rs`, both crates)

In scope only as "note anything." Both bifrost `parse.rs` files are
*better* than the ratatoskr `carddav/parse.rs` shown in the survey
checkout: bifrost uses an element-stack parent check to avoid the
nested-href overwrite bug, gates property commit on 2xx propstat, and
surfaces failed hrefs for destroy-suppression. Ratatoskr's `parse.rs`
(research checkout) uses a flat `current_tag` with no parent context and
would mis-attribute a nested `<href>`. No action needed on bifrost's XML
parsers; they are the stronger implementation. (One tiny nit: bifrost
caldav `is_calendar_resource` is dead-code-adjacent only if content-type
is always present, but it is fine.)

---

## Remediation

Three options, weighed against the bar "not less robust than what
ratatoskr landed."

### Option A: adopt calcard in bifrost

calcard is NOT currently a dependency (confirmed absent from
`Cargo.lock`) and is NOT in the `research/` checkout, so this adds a new
crate (~22k LOC, Apache-2.0/MIT) to two crates that today depend on
neither it nor any iCal/vCard library.

Pros: instantly closes the quoted-param bug, the unfold WSP bug, the
duplicate-DTSTART bug, vCard 3/4 handling, multi-TYPE, group prefixes,
and gives a `Cow`-preserving tokenizer that does not launder untouched
bytes. It is the same parser ratatoskr trusts, so "not less robust"
becomes definitionally true for parsing.

Cons / caveats:
- bifrost re-serializes and must preserve unmodeled lines. calcard's
  parse is great, but bifrost's whole patch model (replace named
  properties, pass the rest through verbatim) is a *byte-preservation*
  task that calcard's typed AST does not directly serve - you would still
  hand-roll the "keep these raw lines, splice these" logic, or adopt
  calcard's serializer and accept its normalization. So calcard removes
  the parse bugs but does not by itself give lossless round-trip; you
  still own the splice.
- calcard's own open bugs are live: #19 (all-day `to_rfc3339()` panics -
  bifrost would have to avoid that API and go through `PartialDateTime`,
  exactly as ratatoskr does) and #14 (RECURRENCE-ID override corner
  case). bifrost only reads RECURRENCE-ID off the master today, so #14 is
  low-exposure, and bifrost does not call `to_rfc3339()`, but adopting
  calcard means inheriting its maintenance-pace risk.
- bifrost needs only the client-projection subset (parse a VEVENT/vCard
  to a handful of fields, splice on update). Pulling in calcard's full
  RRULE engine + JSCalendar + JSContact is a large dependency for a small
  need.

### Option B: targeted hardening of the hand-rolled parsers

Fix the specific defects in place:
1. Fix `unfold_lines` to strip exactly one leading WSP (both copies, then
   dedup into one shared helper). [closes the WSP bug]
2. Stop re-folding/unfolding preserved lines: on the patch path, operate
   on *physical* lines and splice without unfold->refold; only fold lines
   the patch newly emits. [closes the round-trip data-loss bug]
3. Parse quoted parameter values: scan for the unquoted `:` separating
   params from value, honor quoted-strings. [closes the quoted-param bug]
4. Add a duplicate-DTSTART precedence picker mirroring ratatoskr's
   `pick_datetime_entry`. [closes the duplicate bug]
5. vCard: model `N`, ADR PO-box/extended, group prefixes, multi-TYPE; emit
   version-correct PHOTO; reserve the fold-space octet. [closes the vCard
   structured/version bugs]
6. Stop tagging TZID wall-clock times with a false `Z`.
7. Optionally vendor the CLDR windowsZones table (survey catalog #1) for
   Exchange TZID aliasing.

Pros: no new dependency; keeps bifrost's deliberate "thin projection,
verbatim pass-through" design which is genuinely well-suited to a client
that must not lose server bytes; fixes are individually small and
testable; the existing test suite is a good harness to extend.

Cons: it is re-implementing, by hand, things calcard already got right and
fuzzed. New edge cases will keep surfacing (the survey is the evidence
that this is a deep tail). "Not less robust than calcard" is hard to
*prove* with hand-rolled code and no fuzzing.

### Option C: hybrid - calcard for parse, keep the hand-rolled splice serializer

Use `calcard::Parser` for `event_from_ical` / `contact_from_vcard`
(projection-in), keep the current line-splice approach for create/patch
(serialization-out) but fix the unfold/fold WSP and re-fold bugs (items
B1, B2) so the verbatim preservation is actually lossless. This puts the
hardest-to-get-right part (tolerant parsing of adversarial server output)
on the battle-tested crate, while keeping byte-preservation in bifrost's
own splice logic where calcard's typed AST is a poor fit.

### Recommendation

**Option C (hybrid), with Option B's unfold/refold fixes as a
non-negotiable prerequisite regardless of which option is chosen.**

Reasoning:
- The two H-severity round-trip bugs (WSP-stripping unfolder, re-folding
  preserved lines) are pure correctness defects that violate the crate's
  stated contract and will silently corrupt user data on update. They
  must be fixed no matter what, and they are independent of the
  parse-library decision. Do these first.
- For *parsing* adversarial server bodies (quoted params, duplicate
  DTSTART, vCard 3/4, multi-TYPE, Windows TZIDs), matching calcard by hand
  is exactly the losing game the survey documents. Delegating projection-in
  to calcard makes "not less robust than ratatoskr" true by construction
  for the read path, which is where malformed third-party bytes arrive.
- For *serialization-out*, bifrost's requirement is byte-level
  preservation of unmodeled lines, which calcard's typed AST does not
  serve well (you would re-emit through calcard's normalizer and lose
  verbatim fidelity, or hand-roll the splice anyway). Keeping the splice
  serializer - once the fold bugs are fixed - is the right call.
- This caps the new-dependency cost: calcard is pulled in for two parse
  functions, and bifrost never touches calcard's RRULE engine or the
  panicking `to_rfc3339()` path (#19). #14 is near-zero exposure given
  bifrost reads only the master VEVENT.

If the project prefers zero new dependencies, Option B is acceptable but
must include items B1-B6 in full and should add at least a small fuzz
target on the unfold/escape/param-split surface to justify the
"not less robust" claim. Option A (full calcard, including its serializer)
is the most code-deleting but trades away bifrost's verbatim-preservation
property, which is a deliberate and valuable design choice for a sync
client - not recommended.

The single most urgent action, before any library decision: fix
`unfold_lines` to strip one WSP and stop the unfold->refold of preserved
lines. Those are live data-loss bugs today.
