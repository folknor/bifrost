# Remove the `serde` feature

The `serde` feature on `bifrost-imap` has no workspace consumer
enabling it. Inside the crate it is only feature wiring, `cfg_attr`
derives, custom `Error` serde support, and serde-only tests. The crate
does not need serde for protocol parsing or encoding.

It is useful only for downstream app code that wants to serialize IMAP
client value types for things like cache snapshots, IPC payloads,
persisted diagnostics, or JSON logs. Plausible for a general-purpose
public crate, but weak for this crate if bifrost's job is to be the
protocol client and ratatoskr owns app persistence.

The strongest deletion argument: many derived serializers would
quietly turn internal API shape into a persistence format, including
lots of protocol response structs that are still pre-1.0. Keeping that
surface around invites accidental compatibility promises.

Mechanical scope when ready:

- Remove the `serde` feature and optional dep from
  `crates/imap/Cargo.toml:21`.
- Remove `serde_json` dev-dep if no longer used.
- Remove all `#[cfg_attr(feature = "serde", ...)]`.
- Remove the custom `serde_support` module in
  `crates/imap/src/error.rs:512`.
- Remove the gated serde tests in `crates/imap/src/error_tests.rs:915`.

Only real cost is breaking external users who opted into the feature.
Since this is pre-1.0 and the feature is default-off, that cost seems
acceptable.
