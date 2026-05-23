#![forbid(unsafe_code)]
#![doc = "Gmail Account implementation for bifrost."]

pub mod account;
mod api;
mod client;
mod encoding;
mod error;
mod headers;
mod types;

use error::{Error, Result};
