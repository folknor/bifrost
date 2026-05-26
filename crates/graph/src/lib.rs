#![forbid(unsafe_code)]
#![doc = "Microsoft Graph Account implementation for bifrost."]

// pub: sync-engine conformance and consumers register Graph accounts through this module.
pub mod account;
mod api;
mod client;
mod error;
mod ews;
mod types;
mod webhooks;
