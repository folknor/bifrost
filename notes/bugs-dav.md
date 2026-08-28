# bifrost-caldav and bifrost-carddav: hunt findings

Scope: both DAV account crates in full - discovery/home/collection walks,
sync-token and ctag cursor handling, REPORT and PROPFIND parsing, etag
preconditions, mutation paths, and the CalDAV-into-IMAP composition seam.

Hunter note: read `reference/caldav.md`, `reference/carddav.md`,
`notes/dav-parsing-robustness-2026-06-17.md`, and all of
`account.rs`/`client.rs`/`parse.rs` in both crates; skimmed `ical.rs`/`vcard.rs`
only via the reference, since the note says that path was reworked.

Status: no open findings. Every finding raised in this hunt is closed in the
code, and the durable consequences live in `reference/caldav.md`,
`reference/carddav.md` and `reference/imap.md`.
