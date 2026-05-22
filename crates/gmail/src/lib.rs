#![forbid(unsafe_code)]
#![doc = "Gmail API client for Rust."]

// pub: the sync engine and downstream consumers construct Gmail accounts through this module.
pub mod account;
mod api;
// pub: non-engine consumers use the direct Gmail REST facade.
pub mod client;
mod encoding;
// pub: direct REST methods return this crate-specific error type.
pub mod error;
mod headers;
// pub: direct REST methods expose Gmail API wire DTOs.
pub mod types;

// pub: crate-level convenience aliases for direct Gmail REST callers.
pub use error::{Error, Result};
