#![forbid(unsafe_code)]
#![doc = "Microsoft Graph Account implementation for bifrost."]
// The crate-internal `Error` carries Graph response bodies, error envelopes,
// EWS SOAP faults, and webhook validation failures. It is translated to the
// opaque `AccountError` (8 bytes, `Arc<Inner>`-backed) at the protocol
// boundary, so the `result_large_err` concern only applies briefly inside
// this crate.
#![allow(clippy::result_large_err)]

// pub: sync-engine conformance and consumers register Graph accounts through this module.
pub mod account;
mod api;
mod client;
mod error;
mod ews;
mod types;
mod webhooks;
