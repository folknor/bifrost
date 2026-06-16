//! Private shared SASL/SCRAM computation layer for the bifrost protocol crates.
//!
//! This crate is private to bifrost. It is computation-only: no I/O, no async,
//! no protocol command flow. The protocol crates (`bifrost-imap`, and later
//! `bifrost-smtp`) own the wire sequencing - driving `+` continuations, framing
//! commands - and call into the pure transition functions here.
//!
//! The surface is deliberately small: a zeroizing [`Secret`] wrapper, a minimal
//! [`SaslError`], the [`ScramHash`] selector, the SCRAM transition functions,
//! and the CRAM-MD5 response builder. Each protocol crate maps [`SaslError`]
//! back into its own error enum at the call boundary.

#![forbid(unsafe_code)]

mod cram;
mod error;
mod scram;
mod secret;

pub use cram::cram_md5_response;
pub use error::SaslError;
pub use scram::{
    ScramHash, decode_continuation, escape_username, scram_client_final, verify_server_final,
};
pub use secret::Secret;
