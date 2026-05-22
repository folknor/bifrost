#![forbid(unsafe_code)]
#![doc = "Microsoft Graph client for Rust."]

// pub: sync-engine conformance and consumers register Graph accounts through this module.
pub mod account;
mod api;
mod client;
mod ews;
mod types;
mod webhooks;
