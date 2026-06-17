# DAV body parsing robustness: resolved + follow-ups

Date: 2026-06-17

bifrost's CalDAV/CardDAV body handling was hand-rolled and not
robustness-equivalent to what the downstream consumer (ratatoskr) landed
via a parser library. A read-only gap analysis (the original content of
this doc, now in git history) found ~26 findings including live
data-corruption bugs on the update path. The decided remediation was
Option C: adopt the `caldata` crate (v0.15 - the parser inside the
`rustical` CalDAV server and ratatoskr's identified migration target) for
PARSING, and keep bifrost's verbatim-preserving hand-rolled serializer for
create/update, fixing its bugs.

This is implemented across two commits (search the log for
"parse iCalendar via caldata" and "parse vCard via caldata").

## Resolved

Both crates now parse via caldata's low-level content-line tokenizer (NOT
its strict typed builder, so strict-singleton enforcement cannot hard-fail
ingest), unescape raw values at read time, and skip a single malformed
resource via the existing failed-href path instead of terminating the
stream. Closed:

- The WSP-stripping unfolder (stripped all leading whitespace instead of
  one fold octet) - both crates.
- Unfold-then-refold of preserved/unmodeled lines on the patch path
  (the headline data-loss bug) - replaced by a physical-line-group splice
  in both crates, so an unmodeled folded line is byte-preserved.
- Quoted-parameter mis-split (`:`/`;`/`,` inside a quoted value) - both.
- iCal: duplicate-DTSTART silent first-wins (now a precedence picker);
  TZID wall-clock falsely tagged `Z` (now bare value + IANA zone name);
  no Windows/Exchange TZID alias (now mapped via caldata's proprietary
  table); ordering-dependent unescape replace-chain (now single-pass).
- vCard: Apple group-prefix loss on patch (item1.EMAIL / X-ABLabel
  association preserved); PHOTO 3.0-vs-4.0 version correctness and `data:`
  URI handling; multi-TYPE collapse; 3.0-vs-4.0 PREF/TYPE; ORG `;`
  structure; fold-space octet budget; missing N on create.

A positive finding from the analysis (no action): bifrost's DAV XML
`parse.rs` files are stronger than ratatoskr's checkout equivalents
(element-stack parent checks, 2xx commit gating, failed-href
destroy-suppression).

## Remaining follow-ups (each needs a decision or a larger change)

- **bifrost-types structural slots for ADR / ORG.** vCard ADR
  post-office-box / extended-address and ORG sub-structure currently
  round-trip as data but have no dedicated fields on
  `bifrost_types::ContactAddress` / `ContactOrganization`, so their
  structural position is not modeled. Adding the fields touches
  `crates/types` and every contact provider (jmap, google, graph,
  carddav) - a shared-model contract change, deferred for a decision.

- **Real VTIMEZONE offsets (caldav).** Created events still ship a
  conservative fixed `+0000` VTIMEZONE stub. A correct offset needs either
  caldata's `vtimezones-rs` feature or a `chrono-tz` dependency plus the
  now-canonicalized IANA zone name. New-dependency decision; current
  behavior does not regress (a strict server re-resolves by TZID name).

- **RDATE/EXDATE multi-value modeling (caldav).** A single
  RDATE/EXDATE property carrying a comma-separated date list (or RDATE
  PERIOD) is treated as one opaque token. Lossless on pass-through
  (the string is preserved); a modeling gap only, for any consumer that
  wants one entry per date.

## Deliberately kept as-is

- iCal projects only the first VEVENT (recurrence overrides preserved
  verbatim on patch, recurrence-replace refused when overrides exist) -
  intentional, guarded.
- empty-vs-absent property values are not distinguished - the same
  limitation calcard has (only caldata-rs's strict typed layer solves it,
  which bifrost does not use to avoid hard-failing ingest). Noted in
  carddav.md.
- iCal `escape_text` deletes a bare CR rather than converting it - minor.
