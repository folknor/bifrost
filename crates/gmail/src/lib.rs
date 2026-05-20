#![forbid(unsafe_code)]
#![doc = "Gmail API client for Rust."]

pub mod api;
pub mod auth_parser;
pub mod blob;
pub mod client;
pub mod contacts;
pub mod encoding;
pub mod error;
pub mod gdrive;
pub mod headers;
pub mod message;
pub mod parse;
pub mod types;

pub use error::{Error, Result};
